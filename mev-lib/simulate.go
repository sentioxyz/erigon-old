package mev

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth"
)

type MessageType core.Message

type SimulateContext interface {
	PrepareForTx(blockHash, txHash common.Hash, txIndex int, message MessageType)

	Execute() (*core.ExecutionResult, error)
}

type gethSimulationContext struct {
	statedb *state.IntraBlockState
	eth     *eth.Ethereum

	ctx        context.Context
	isEIP158   bool
	vmContext  evmtypes.BlockContext
	gp         *core.GasPool
	trace      *SimulateTracer
	preparedTx core.Message
	txContext  evmtypes.TxContext
}

func PrepareStateDBForTx(statedb *state.IntraBlockState, _, thash common.Hash, idx int) {
	statedb.SetTxContext(thash, idx)
}

func MakeVMConfig(trace vm.EVMLogger) vm.Config {
	return vm.Config{
		Tracer:    trace,
		NoBaseFee: true,
	}
}

func MessageGasLimit(m MessageType) uint64 {
	return m.Gas()
}

func VMApplyMessage(vmenv *vm.EVM, msg MessageType, gp *core.GasPool) (*core.ExecutionResult, error) {
	return core.ApplyMessage(vmenv, msg, gp, false, true)
}

func (ec *gethSimulationContext) PrepareForTx(
	blockHash, txHash common.Hash, txIndex int, message MessageType) {
	ec.txContext = core.NewEVMTxContext(message)
	ec.preparedTx = message
	PrepareStateDBForTx(ec.statedb, txHash, blockHash, txIndex)
}

func (ec *gethSimulationContext) Execute() (*core.ExecutionResult, error) {
	var vmConfig vm.Config
	if ec.trace == nil {
		vmConfig = MakeVMConfig(nil)
	} else {
		vmConfig = MakeVMConfig(ec.trace)
	}

	vmenv := vm.NewEVM(ec.vmContext, ec.txContext, ec.statedb, ec.eth.ChainConfig(), vmConfig)
	if ec.trace != nil {
		ec.trace.CaptureTxStart(MessageGasLimit(ec.preparedTx))
	}
	if result, err := VMApplyMessage(vmenv, ec.preparedTx, ec.gp); err != nil {
		if ec.trace != nil {
			ec.trace.Stop(err)
		}
		return nil, err
	} else {
		_ = ec.statedb.FinalizeTx(
			ec.eth.ChainConfig().Rules(ec.vmContext.BlockNumber, ec.vmContext.Time),
			state.NewNoopWriter())
		if ec.trace != nil {
			ec.trace.CaptureTxEndMEV(result.UsedGas, result.Err, result.ReturnData)
		}
		return result, nil
	}
}

func CreateSimulationContext(
	ctx context.Context,
	eth *eth.Ethereum,
	tx kv.Getter,
	abiType string,
	statedb *state.IntraBlockState,
	blockHeader *types.Header,
	tracer *SimulateTracer,
) (SimulateContext, error) {
	switch abiType {
	case ABITypeGeth:
		headerReader := eth.BlockIO()
		getHeader := func(hash common.Hash, n uint64) *types.Header {
			h, _ := headerReader.HeaderByNumber(ctx, tx, n)
			return h
		}
		return &gethSimulationContext{
			statedb:   statedb,
			eth:       eth,
			ctx:       ctx,
			isEIP158:  true,
			vmContext: core.NewEVMBlockContext(blockHeader, core.GetHashFn(blockHeader, getHeader), eth.Engine(), nil),
			gp:        new(core.GasPool).AddGas(math.MaxUint64),
			trace:     tracer,
		}, nil
	default:
		return nil, fmt.Errorf("unknown abi type: %s", abiType)
	}
}
