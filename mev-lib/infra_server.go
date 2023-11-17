package mev

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	ABITypeGeth = "geth"
)

var (
	mockAddress = libcommon.BytesToAddress([]byte("0x7777788888999990000011111222223333344444"))

	defaultChannelSize = 65536
)

type preparedSimulateEnv struct {
	stateOverrides      []*api.StateOverride
	knownCodeHashes     []libcommon.Hash
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
	blockHash, ok := block["hash"].(libcommon.Hash)
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
			libcommon.HexToAddress(string(req.Address[i])),
			string(req.StorageKey[i]),
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
	overrides := map[libcommon.Address]ethapi2.Account{}
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
		to := libcommon.BytesToAddress(req.GetAddress())
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

func (s *InfraServer) SimulateSingleTxWithErigon(req *api.SimulateRequest, ss api.MEVInfra_SimulateServer) error {
	ctx := ss.Context()
	if len(req.Bundle) > 1 {
		return ss.Send(&api.SimulateResponse{
			Trace: &api.SimulateResponse_Stop{
				Stop: fmt.Sprintf("only one tx is allowed in a bundle"),
			},
		})
	}
	var err error
	var txHash libcommon.Hash
	switch op := req.Bundle[0]; op.Type {
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
	case api.SimulateTxType_SIMULATE_TXHASH:
		txHash = libcommon.HexToHash(string(op.Data))
	case api.SimulateTxType_SIMULATE_RAW_MESSAGE:
		err = fmt.Errorf("raw message is not supported")
	default:
		return fmt.Errorf("unknown tx type: %s", op.Type)
	}
	result, err := s.traceBackend.ReplayTransaction(ctx, txHash, []string{"trace", "stateDiff"})
	if err != nil {
		return ss.Send(&api.SimulateResponse{
			Trace: &api.SimulateResponse_Stop{
				Stop: err.Error(),
			},
		})
	}
	if err = ss.Send(&api.SimulateResponse{
		Trace: &api.SimulateResponse_TxStart{
			TxStart: &api.EvmTxStart{},
		},
	}); err != nil {
		log.Error("failed to send response", "err", err)
		return err
	}
	for acc, stateDiff := range result.StateDiff {
		for key, value := range stateDiff.Storage {
			v, ok := value["*"].(*commands.StateDiffStorage)
			if !ok {
				continue
			}
			if err = ss.Send(&api.SimulateResponse{
				Trace: &api.SimulateResponse_StateDiff{
					StateDiff: &api.StateDiff{
						Address: acc.Bytes(),
						Diff: &api.StateDiff_StorageKey{
							StorageKey: key.Bytes(),
						},
						ValueBefore: v.From.Bytes(),
						ValueAfter:  v.To.Bytes(),
					},
				},
			}); err != nil {
				log.Error("failed to send response", "err", err)
				return err
			}
		}
	}
	if err = ss.Send(&api.SimulateResponse{
		Trace: &api.SimulateResponse_TxEnd{
			TxEnd: &api.EvmTxEnd{},
		},
	}); err != nil {
		log.Error("failed to send response", "err", err)
		return err
	}
	return nil
}

func (s *InfraServer) SimulateManyWithErigon(req *api.SimulateRequest, ss api.MEVInfra_SimulateServer) error {
	ctx := ss.Context()
	respChan := make(chan *api.SimulateResponse, defaultChannelSize)
	tracer := NewSimulateTracer(req, respChan)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case resp, ok := <-respChan:
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
		defer close(respChan)
		var txs []types.Transaction
		for _, op := range req.GetBundle() {
			var err error
			var tx types.Transaction
			switch op.Type {
			case api.SimulateTxType_SIMULATE_TXDATA:
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
			case api.SimulateTxType_SIMULATE_TXHASH:
				txHash := libcommon.HexToHash(string(op.Data))
				var rpcTx hexutil.Bytes
				rpcTx, err = s.ethBackend.GetRawTransactionByHash(ctx, txHash)
				if err != nil {
					log.Error("failed to get raw transaction", "err", err)
					return err
				}
				tx, err = types.UnmarshalTransactionFromBinary(rpcTx)
				if err != nil {
					log.Error("failed to unmarshal transaction", "err", err)
				}
			case api.SimulateTxType_SIMULATE_RAW_MESSAGE:
				err = fmt.Errorf("raw message is not supported")
			default:
				return fmt.Errorf("unknown tx type: %s", op.Type)
			}
			if err != nil {
				log.Error("failed to get transaction", "err", err)
			}
			txs = append(txs, tx)
		}
		blockNumber := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(req.GetStateBlockNumber()))
		var stateOverrides []*api.StateOverride
		var knownCodeHashes []libcommon.Hash
		for _, override := range req.GetStateOverrides() {
			stateOverrides = append(stateOverrides, override)
			knownCodeHashes = append(knownCodeHashes, libcommon.Hash{})
		}
		if req.GetPreparedSimulateEnv() != "" {
			s.mu.RLock()
			preparedEnv, ok := s.preparedSimulateEnvMap[req.PreparedSimulateEnv]
			s.mu.RUnlock()
			if !ok {
				return fmt.Errorf("prepared simulate env not found: %s", req.PreparedSimulateEnv)
			}
			stateOverrides = append(stateOverrides, preparedEnv.stateOverrides...)
			knownCodeHashes = append(knownCodeHashes, preparedEnv.knownCodeHashes...)
		}
		_, err := s.traceBackend.MEVCallMany(ctx, tracer,
			[]string{"mevTrace"}, txs, &blockNumber, stateOverrides, req.GetBlockOverrides(), knownCodeHashes)
		if err != nil {
			log.Error("failed to simulate", "err", err)
			return err
		}
		return nil
	})
	return g.Wait()
}

func (s *InfraServer) Simulate(req *api.SimulateRequest, ss api.MEVInfra_SimulateServer) error {
	// s.SimulateSingleTxWithErigon(req, ss)
	return s.SimulateManyWithErigon(req, ss)
}

func applyStateOverride(statedb *state.IntraBlockState, override *api.StateOverride, knownCodeHash *libcommon.Hash) {
	address := libcommon.BytesToAddress(override.Address)
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
			k := libcommon.BytesToHash([]byte(key))
			v := new(uint256.Int).SetBytes(value)
			statedb.SetState(address, &k, *v)
		}
	}
	if override.Nonce > 0 {
		statedb.SetNonce(address, override.Nonce)
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
		knownCodeHashes: make([]libcommon.Hash, len(overrides)),
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
