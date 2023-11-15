package mev

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/mev-lib/api"
	"github.com/ledgerwatch/erigon/rpc"
	ethapi2 "github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

const (
	ABITypeGeth = "geth"
)

var (
	mockAddress = common.BytesToAddress([]byte("0x7777788888999990000011111222223333344444"))

	defaultChannelSize = 65536
)

type preparedSimulateEnv struct {
	stateOverrides      []*api.StateOverride
	knownCodeHashes     []common.Hash
	lastAccessTimestamp int64
}

type InfraServer struct {
	*api.UnimplementedMEVInfraServer

	mu                     sync.RWMutex
	preparedSimulateEnvMap map[string]*preparedSimulateEnv

	eth           *eth.Ethereum
	ethBackend    commands.EthAPI
	erigonBackend commands.ErigonAPI
	txpoolBackend commands.TxPoolAPI
	netBackend    commands.NetAPI
	traceBackend  commands.TraceAPI
	debugBackend  commands.PrivateDebugAPI
	web3Backend   commands.Web3API
	borBackend    commands.BorAPI
	otsBackend    commands.OtterscanAPI
	adminBackend  commands.AdminAPI
}

func NewInfraServer(eth *eth.Ethereum, apiList []rpc.API) *InfraServer {
	s := &InfraServer{
		eth:                         eth,
		UnimplementedMEVInfraServer: &api.UnimplementedMEVInfraServer{},
	}
	for _, a := range apiList {
		switch a.Namespace {
		case "eth":
			s.ethBackend = a.Service.(commands.EthAPI)
		case "erigon":
			s.erigonBackend = a.Service.(commands.ErigonAPI)
		case "txpool":
			s.txpoolBackend = a.Service.(commands.TxPoolAPI)
		case "net":
			s.netBackend = a.Service.(commands.NetAPI)
		case "trace":
			s.traceBackend = a.Service.(commands.TraceAPI)
		case "debug":
			s.debugBackend = a.Service.(commands.PrivateDebugAPI)
		case "web3":
			s.web3Backend = a.Service.(commands.Web3API)
		case "bor":
			s.borBackend = a.Service.(commands.BorAPI)
		case "ots":
			s.otsBackend = a.Service.(commands.OtterscanAPI)
		case "admin":
			s.adminBackend = a.Service.(commands.AdminAPI)
		default:
			log.Info("Unknown API namespace", "namespace", a.Namespace)
		}
	}
	return s
}

func (s *InfraServer) HistoricalState(ctx context.Context, req *api.HistoricalStateRequest) (*api.HistoricalStateResponse, error) {
	if s.ethBackend == nil {
		log.Error("eth API is not available")
		return nil, fmt.Errorf("eth API is not available")
	}
	if len(req.StorageKey) != len(req.Address) {
		log.Error("address and storage key length mismatch")
		return nil, fmt.Errorf("address and storage key length mismatch")
	}
	block, err := s.ethBackend.GetBlockByNumber(ctx, rpc.BlockNumber(req.BlockNumber), false)
	if err != nil {
		log.Error("failed to get block", "err", err, "block", req.BlockNumber)
		return nil, err
	}
	if block == nil {
		log.Error("block not found", "block", req.BlockNumber)
		return nil, errors.New("block re-org detected")
	}
	blockHash, ok := block["hash"].(common.Hash)
	if !ok {
		log.Error("invalid block", "block", block)
		return nil, fmt.Errorf("invalid block: %v", block)
	}
	resp := &api.HistoricalStateResponse{
		BlockHash: blockHash.Bytes(),
	}
	for i := 0; i < len(req.Address); i++ {
		storage, err := s.ethBackend.GetStorageAt(
			ctx,
			common.BytesToAddress(req.Address[i]),
			common.BytesToHash(req.StorageKey[i]).String(),
			rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(req.BlockNumber)))
		if err != nil {
			log.Error("failed to get storage", "err", err, "address", req.Address[i], "key", req.StorageKey[i])
			return nil, err
		}
		resp.Value = append(resp.Value, []byte(storage))
	}
	return resp, nil
}

func (s *InfraServer) EthCall(ctx context.Context, req *api.EthCallRequest) (
	*api.EthCallResponse, error) {
	callArgs := ethapi2.CallArgs{}
	maxGas := hexutil.Uint64(math.MaxInt64)
	overrides := map[common.Address]ethapi2.Account{}
	data := hexutil.Bytes(req.Data)
	switch {
	case req.GetCode() != nil:
		callArgs.Gas = &maxGas
		callArgs.To = &mockAddress
		callArgs.Data = &data
		code := req.GetCode()
		overrides[mockAddress] = ethapi2.Account{
			Code: (*hexutil.Bytes)(&code),
		}
	case req.GetAddress() != nil:
		callArgs.Gas = &maxGas
		to := common.BytesToAddress(req.GetAddress())
		callArgs.To = &to
		callArgs.Data = &data
	default:
		return nil, fmt.Errorf("invalid request: must specify either code or address")
	}
	stateOverrides := ethapi2.StateOverrides(overrides)
	block := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(req.GetBlockNumber()))

	result, err := s.ethBackend.Call(ctx,
		callArgs,
		block,
		&stateOverrides)
	if err != nil {
		return nil, err
	}
	gasUsed, err := s.ethBackend.EstimateGas(ctx,
		&callArgs,
		&block)
	if err != nil {
		return nil, err
	}
	return &api.EthCallResponse{
		Output:  result,
		GasUsed: uint64(gasUsed),
	}, nil
}

func (s *InfraServer) Simulate(req *api.SimulateRequest, ss api.MEVInfra_SimulateServer) error {
	ctx := ss.Context()
	abiType := ABITypeGeth
	sendCh := make(chan *api.SimulateResponse, defaultChannelSize)
	tracer := &SimulateTracer{
		req:              req,
		sendCh:           sendCh,
		includeLogs:      req.IncludeLogs,
		includeStateDiff: req.IncludeStateDiff,
	}
	var e *simulateEnv
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case resp, ok := <-sendCh:
				if !ok {
					return nil
				}
				if err := ss.Send(resp); err != nil {
					log.Error("failed to send response", "err", err)
					return err
				}
			}
		}
	})
	g.Go(func() error {
		defer close(sendCh)
		var messages []MessageType
		var txHashes []common.Hash
		var xorTxHash common.Hash
		return s.withSimulateEnv(ctx, uint64(req.StateBlockNumber), abiType, tracer,
			func(env *simulateEnv) error {
				e = env
				tracer.statedb = e.statedb
				statedb, header := e.statedb, e.header
				if req.BlockOverrides != nil {
					if req.BlockOverrides.BlockNumber > 0 {
						header.Number = new(big.Int).SetUint64(req.BlockOverrides.BlockNumber)
					}
					if req.BlockOverrides.Timestamp > 0 {
						header.Time = uint64(req.BlockOverrides.Timestamp)
					}
					if len(req.BlockOverrides.Coinbase) > 0 {
						header.Coinbase = common.BytesToAddress(req.BlockOverrides.Coinbase)
					}
					if req.BlockOverrides.Difficulty > 0 {
						header.Difficulty = new(big.Int).SetUint64(req.BlockOverrides.Difficulty)
					}
					if req.BlockOverrides.BaseFee > 0 {
						header.BaseFee = new(big.Int).SetUint64(req.BlockOverrides.BaseFee)
					}
				}
				if req.PreparedSimulateEnv != "" {
					s.mu.RLock()
					preparedEnv, ok := s.preparedSimulateEnvMap[req.PreparedSimulateEnv]
					s.mu.RUnlock()
					if !ok {
						return fmt.Errorf("prepared simulate env not found: %s", req.PreparedSimulateEnv)
					}
					preparedEnv.ApplyTo(statedb, header)
				}
				if len(req.StateOverrides) > 0 {
					for _, override := range req.StateOverrides {
						applyStateOverride(statedb, override, nil)
					}
				}

				signer := types.MakeSigner(s.eth.ChainConfig(), header.Number.Uint64())
				for _, op := range req.Bundle {
					var txHash common.Hash
					var msg MessageType
					var err error

					switch op.Type {
					case api.SimulateTxType_SIMULATE_TXDATA:
						var tx types.Transaction
						switch op.Encoding {
						case api.TxEncoding_TX_ENCODING_RLP:
							tx, err = types.UnmarshalTransactionFromBinary(op.Data)
						default:
							err = fmt.Errorf("unknown tx encoding: %s", op.Encoding)
						}
						if err != nil {
							log.Error("failed to unmarshal transaction", "err", err)
							return err
						}
						txHash = tx.Hash()
						msg, err = tx.AsMessage(*signer, header.BaseFee,
							s.eth.ChainConfig().Rules(header.Number.Uint64(), header.Time))
					case api.SimulateTxType_SIMULATE_TXHASH:
						var tx types.Transaction
						var rpcTx hexutil.Bytes
						hash := common.HexToHash(string(op.Data))
						rpcTx, err = s.ethBackend.GetRawTransactionByHash(ctx, hash)
						if err != nil {
							log.Error("failed to get raw transaction", "err", err)
							return err
						}
						tx, err = types.UnmarshalTransactionFromBinary(rpcTx)
						if err != nil {
							log.Error("failed to unmarshal transaction", "err", err)
							return err
						}
						txHash = tx.Hash()
						msg, err = tx.AsMessage(*signer, header.BaseFee,
							s.eth.ChainConfig().Rules(header.Number.Uint64(), header.Time))
					case api.SimulateTxType_SIMULATE_RAW_MESSAGE:
						err = fmt.Errorf("raw message is not supported")
					default:
						return fmt.Errorf("unknown tx type: %s", op.Type)
					}
					if err != nil {
						log.Error("failed to create message", "err", err)
						return err
					}
					messages = append(messages, msg)
					txHashes = append(txHashes, txHash)
					for i := range xorTxHash {
						xorTxHash[i] ^= txHash[i]
					}
				}
				if len(messages) == 0 {
					return errors.New("no messages to simulate")
				}
				return nil
			},
			func(env *simulateEnv) error {
				for idx, message := range messages {
					e.ec.PrepareForTx(common.Hash{}, txHashes[idx], idx, message)
					if _, err := e.ec.Execute(); err != nil {
						log.Error("failed to simulate", "tx", txHashes[idx].Hex(), "err", err)
						return err
					}
				}
				return nil
			},
		)
	})
	return g.Wait()
}

func applyStateOverride(statedb *state.IntraBlockState, override *api.StateOverride, knownCodeHash *common.Hash) {
	address := common.BytesToAddress(override.Address)
	if len(override.Balance) > 0 {
		statedb.SetBalance(address, new(uint256.Int).SetBytes(override.Balance))
	}
	if len(override.Code) > 0 {
		if knownCodeHash != nil {
			statedb.SetCodeWithHashKnown(address, *knownCodeHash, override.Code)
		} else {
			statedb.SetCode(address, override.Code)
		}
	}
	if len(override.Storage) > 0 {
		for key, value := range override.Storage {
			k := common.BytesToHash([]byte(key))
			v := new(uint256.Int).SetBytes(value)
			statedb.SetState(address, &k, *v)
		}
	}
	if override.Nonce > 0 {
		statedb.SetNonce(address, uint64(override.Nonce))
	}
}

func (env *preparedSimulateEnv) ApplyTo(statedb *state.IntraBlockState, header *types.Header) {
	env.lastAccessTimestamp = time.Now().Unix() // No lock needed.
	for i, override := range env.stateOverrides {
		if len(override.Code) > 0 {
			applyStateOverride(statedb, override, &env.knownCodeHashes[i])
		} else {
			applyStateOverride(statedb, override, nil)
		}
	}
}

func MakePreparedSimulateEnv(overrides []*api.StateOverride) *preparedSimulateEnv {
	env := &preparedSimulateEnv{
		stateOverrides:  overrides,
		knownCodeHashes: make([]common.Hash, len(overrides)),
	}
	for i, override := range overrides {
		if len(override.Code) == 0 {
			continue
		}
		env.knownCodeHashes[i] = crypto.Keccak256Hash(override.Code)
	}
	return env
}

func (s *InfraServer) PrepareSimulateEnv(ctx context.Context, req *api.PrepareSimulateEnvRequest) (*api.PrepareSimulateEnvResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.preparedSimulateEnvMap == nil {
		s.preparedSimulateEnvMap = make(map[string]*preparedSimulateEnv)
	} else {
		// Cleanup old entries.
		now := time.Now().Unix()
		for name, env := range s.preparedSimulateEnvMap {
			if now-env.lastAccessTimestamp > 24*60*60 {
				delete(s.preparedSimulateEnvMap, name)
			}
		}
	}
	// Generate an uuid as name.
	name := uuid.New().String()
	s.preparedSimulateEnvMap[name] = MakePreparedSimulateEnv(req.StateOverrides)
	return &api.PrepareSimulateEnvResponse{
		PreparedSimulateEnv: name,
	}, nil
}

func (s *InfraServer) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (*types.Block, error) {
	if blockNumber == rpc.PendingBlockNumber {
		return nil, nil
	}

	tx, err := s.eth.ChainDB().BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockReader := s.eth.BlockIO()
	block, _, err := blockReader.BlockWithSenders(ctx, tx, common.Hash{}, uint64(blockNumber.Int64()))
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", uint64(blockNumber.Int64()))
	}
	return block, nil
}
