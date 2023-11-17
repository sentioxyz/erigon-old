package mev

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/mev-lib/api"
)

const (
	memoryPadLimit = 1024 * 1024
)

// GetMemoryCopyPadded returns offset + size as a new slice.
// It zero-pads the slice if it extends beyond memory bounds.
func GetMemoryCopyPadded(m *vm.Memory, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, errors.New("offset or size must not be negative")
	}
	if int(offset+size) < m.Len() { // slice fully inside memory
		return m.GetCopy(offset, size), nil
	}
	paddingNeeded := int(offset+size) - m.Len()
	if paddingNeeded > memoryPadLimit {
		return nil, fmt.Errorf("reached limit for padding memory slice: %d", paddingNeeded)
	}
	cpy := make([]byte, size)
	if overlap := int64(m.Len()) - offset; overlap > 0 {
		copy(cpy, m.GetPtr(offset, overlap))
	}
	return cpy, nil
}

type SimulateTracer struct {
	req              *api.SimulateRequest
	sendCh           chan *api.SimulateResponse
	includeLogs      bool
	includeStateDiff bool
}

func NewSimulateTracer(req *api.SimulateRequest, sendCh chan *api.SimulateResponse) *SimulateTracer {
	return &SimulateTracer{
		req:              req,
		sendCh:           sendCh,
		includeLogs:      req.IncludeLogs,
		includeStateDiff: req.IncludeStateDiff,
	}
}

var _ vm.EVMLogger = &SimulateTracer{}

func (t *SimulateTracer) handleLog(topics [][]byte, d []byte) {
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_Log{
			Log: &api.Log{
				Topics: topics,
				Data:   d,
			},
		},
	}
}

func (t *SimulateTracer) handleStateDiff(caller []byte, key []byte, valueAfter []byte) {
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_StateDiff{
			StateDiff: &api.StateDiff{
				Address: caller[:],
				Diff: &api.StateDiff_StorageKey{
					StorageKey: key[:],
				},
				// ValueBefore: value_before.Bytes(),
				ValueAfter: valueAfter[:],
			},
		},
	}
}

func (t *SimulateTracer) handleRevert(data []byte) {
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_Revert{
			Revert: data,
		},
	}
}

func (t *SimulateTracer) CaptureTxStart(gasLimit uint64) {
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_TxStart{
			TxStart: &api.EvmTxStart{
				GasLimit: gasLimit,
			},
		},
	}
}

func (t *SimulateTracer) CaptureTxEnd(restGas uint64) {
}

func (t *SimulateTracer) CaptureTxEndMEV(usedGas uint64, err error, returnData []byte) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_TxEnd{
			TxEnd: &api.EvmTxEnd{
				UsedGas:    usedGas,
				Err:        errStr,
				ReturnData: returnData,
			},
		},
	}
}

func (t *SimulateTracer) CaptureStart(env vm.VMInterface,
	from common.Address, to common.Address,
	precompile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_CallStart{
			CallStart: &api.EvmCallStart{
				From:   from.Bytes(),
				To:     to.Bytes(),
				Create: create,
				Input:  input,
				Gas:    gas,
				Value:  value.Bytes(),
			},
		},
	}
}

func (t *SimulateTracer) CaptureEnter(typ vm.OpCode,
	from common.Address, to common.Address,
	precompile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	var valueBytes []byte
	if value != nil {
		valueBytes = value.Bytes()
	}
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_Enter{
			Enter: &api.EvmEnter{
				From:       from.Bytes(),
				To:         to.Bytes(),
				Input:      input,
				Gas:        gas,
				Value:      valueBytes,
				OpcodeType: uint32(typ),
			},
		},
	}
}

func (t *SimulateTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_Exit{
			Exit: &api.EvmExit{
				Output:  output,
				GasUsed: gasUsed,
				Err:     errMsg,
			},
		},
	}
}

func (t *SimulateTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_CallEnd{
			CallEnd: &api.EvmCallEnd{
				Output:  output,
				GasUsed: gasUsed,
				Err:     errMsg,
			},
		},
	}
}

func (t *SimulateTracer) CaptureState(pc uint64, op vm.OpCode,
	gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	for _, hook := range t.req.OpCodeHooks {
		if hook.OpCode == uint32(op) {
			t.sendCh <- &api.SimulateResponse{
				Trace: &api.SimulateResponse_OpCodeTrace{
					OpCodeTrace: &api.OpCodeTrace{
						OpCode: uint32(op),
					},
				},
			}
		}
	}

	stack := scope.Stack
	stackData := stack.Data
	stackLen := len(stackData)
	captureLog := func(size int) {
		if !t.includeLogs {
			return
		}
		topics := make([][]byte, size)
		ptr := stackLen
		mStart, mSize := stackData[ptr-1], stackData[ptr-2]
		ptr -= 2
		for i := 0; i < size; i++ {
			addr := stackData[ptr-1]
			topics[i] = addr.Bytes()
			ptr -= 1
		}
		d, err := GetMemoryCopyPadded(scope.Memory, int64(mStart.Uint64()), int64(mSize.Uint64()))
		if err != nil {
			return
		}
		t.handleLog(topics, d)
	}
	switch {
	case t.includeStateDiff && stackLen >= 2 && op == vm.SSTORE:
		caller := scope.Contract.Address()
		key := stackData[stackLen-1].Bytes32()
		valueAfter := stackData[stackLen-2].Bytes32()
		t.handleStateDiff(caller[:], key[:], valueAfter[:])
	case stackLen >= 2 && op == vm.REVERT:
		offset, size := stackData[stackLen-1], stackData[stackLen-2]
		ret := scope.Memory.GetPtr(int64(offset.Uint64()), int64(size.Uint64()))
		t.handleRevert(ret)
	case op == vm.LOG0:
		captureLog(0)
	case op == vm.LOG1:
		captureLog(1)
	case op == vm.LOG2:
		captureLog(2)
	case op == vm.LOG3:
		captureLog(3)
	case op == vm.LOG4:
		captureLog(4)
	}
}

func (t *SimulateTracer) CaptureFault(pc uint64,
	op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *SimulateTracer) OnCreate() {
	t.includeLogs = true
}

func (t *SimulateTracer) Stop(err error) {
	t.sendCh <- &api.SimulateResponse{
		Trace: &api.SimulateResponse_Stop{
			Stop: err.Error(),
		},
	}
}

func (t *SimulateTracer) CaptureSystemTxEnd(intrinsicGas uint64) {}

func (t *SimulateTracer) CapturePreimage(pc uint64, hash common.Hash, preimage []byte) {}
