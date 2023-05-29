package sentio

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracer)
}

type sentioTracer struct {
	//internalCall bool
	env               vm.VMInterface
	activePrecompiles []libcommon.Address

	traces       []Trace
	descended    bool
	index        int
	currentDepth int
	currentGas   uint64
	rootTrace    Trace
}

type Trace struct {
	op      vm.OpCode
	Type    string            `json:"type"` //  this will be differ than js version
	Pc      uint64            `json:"pc"`
	Index   int               `json:"index"`
	GasIn   uint64            `json:"gasIn"`
	Gas     uint64            `json:"gas"`
	GasCost uint64            `json:"gasCost"`
	GasUsed uint64            `json:"gasUsed"`
	Output  hexutil.Bytes     `json:"output,omitempty"`
	From    libcommon.Address `json:"from,omitempty"`

	// Used by call
	To          libcommon.Address `json:"to,omitempty"`
	Input       hexutil.Bytes     `json:"input,omitempty"`
	Value       hexutil.Bytes     `json:"value,omitempty"`
	ErrorString string            `json:"error,omitempty"`

	// Used by jump
	Stack  []uint256.Int `json:"stack,omitempty"`
	Memory []byte        `json:"memory,omitempty"`

	// Used by log
	Address libcommon.Address `json:"address,omitempty"`
	Data    hexutil.Bytes     `json:"data,omitempty"`
	Topics  []hexutil.Bytes   `json:"topics,omitempty"`

	// Only used by root
	Traces []Trace `json:"traces,omitempty"`
}

func (t *sentioTracer) CaptureTxStart(gasLimit uint64) {}
func (t *sentioTracer) CaptureTxEnd(restGas uint64)    {}
func (t *sentioTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env
	// Update list of precompiles based on current block
	rules := env.ChainConfig().Rules(env.Context().BlockNumber, env.Context().Time)
	t.activePrecompiles = vm.ActivePrecompiles(rules)
}
func (t *sentioTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	t.rootTrace.GasUsed = usedGas
	t.rootTrace.Output = output
	t.rootTrace.Traces = t.traces
}
func (t *sentioTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.rootTrace = Trace{
		Index: 0,
		Type:  typ.String(),
		From:  from,
		To:    to,
		Gas:   gas,
		Input: input,
	}
}
func (t *sentioTracer) CaptureExit(output []byte, usedGas uint64, err error) {}

func (t *sentioTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Capture any errors immediately
	if err != nil {
		t.traces = append(t.traces, Trace{
			Type:        "ERROR",
			ErrorString: err.Error(), //TODO ask pengcheng
		})
		return
	}

	traceBase := Trace{
		Pc:      pc,
		Type:    op.String(),
		Index:   t.index,
		GasIn:   gas,
		GasCost: cost,
	}
	t.index++
	//topIdx := len(t.traces) - 1
	var mergeBase = func(trace Trace) Trace {
		trace.op = traceBase.op
		trace.Pc = traceBase.Pc
		trace.Type = traceBase.Type
		trace.Index = traceBase.Index
		trace.GasIn = traceBase.GasIn
		trace.GasCost = traceBase.GasCost
		return trace
	}

	switch op {
	case vm.RETURN:
		outputOffset := scope.Stack.Peek()
		outputSize := scope.Stack.Back(1)
		// TODO check if we should use getPtr or getCopy
		trace := mergeBase(Trace{
			Value: scope.Memory.GetPtr(int64(outputOffset.Uint64()), int64(outputSize.Uint64())),
		})
		t.traces = append(t.traces, trace)
		return
	case vm.CREATE:
		fallthrough
	case vm.CREATE2:
		inputOffset := scope.Stack.Back(1)
		inputSize := scope.Stack.Back(2)
		// TODO calculate to
		call := mergeBase(Trace{
			From:  scope.Contract.Address(),
			Input: scope.Memory.GetPtr(int64(inputOffset.Uint64()), int64(inputSize.Uint64())),
			Value: scope.Stack.Peek().Bytes(),
		})
		t.traces = append(t.traces, call)
		t.descended = true
		return
	case vm.SELFDESTRUCT:
		from := scope.Contract.Address()
		call := mergeBase(Trace{
			From:  from,
			To:    libcommon.BytesToAddress(scope.Stack.Peek().Bytes()),
			Value: t.env.IntraBlockState().GetBalance(from).Bytes(),
		})
		t.traces = append(t.traces, call)
		return
	case vm.CALL:
		fallthrough
	case vm.CALLCODE:
		fallthrough
	case vm.DELEGATECALL:
		fallthrough
	case vm.STATICCALL:
		to := libcommon.BytesToAddress(scope.Stack.Back(1).Bytes())
		if t.isPrecompiled(to) {
			return
		}
		offset := 1
		if op == vm.DELEGATECALL || op == vm.STATICCALL {
			offset = 0
		}
		inputOffset := scope.Stack.Back(offset + 2)
		inputSize := scope.Stack.Back(offset + 3)
		call := mergeBase(Trace{
			From:  scope.Contract.Address(),
			To:    to,
			Input: scope.Memory.GetPtr(int64(inputOffset.Uint64()), int64(inputSize.Uint64())),
		})
		if op == vm.CALL || op == vm.CALLCODE {
			call.Value = scope.Stack.Back(2).Bytes()
		}
		t.traces = append(t.traces, call)
		t.descended = true
		return
	case vm.JUMP:
		fallthrough
	case vm.JUMPI:
		fallthrough
	case vm.JUMPDEST:
		jump := mergeBase(Trace{
			Stack: append([]uint256.Int(nil), scope.Stack.Data...),
			//Memory: scope.Memory.Data(),
		})
		t.traces = append(t.traces, jump)
		return
	case vm.LOG0:
		fallthrough
	case vm.LOG1:
		fallthrough
	case vm.LOG2:
		fallthrough
	case vm.LOG3:
		fallthrough
	case vm.LOG4:
		topicCount := 0xf & op
		logOffset := scope.Stack.Peek()
		logSize := scope.Stack.Back(1)
		data := scope.Memory.GetPtr(int64(logOffset.Uint64()), int64(logSize.Uint64()))
		var topics []hexutil.Bytes
		//stackLen := scope.Stack.Len()
		for i := 0; i < int(topicCount); i++ {
			topics = append(topics, scope.Stack.Back(2+i).Bytes())
		}
		l := mergeBase(Trace{
			Address: scope.Contract.Address(),
			Data:    data,
			Topics:  topics,
		})
		t.traces = append(t.traces, l)
		return
	}

	// If we've just descended into an inner call, retrieve it's true allowance. We
	// need to extract if from within the call as there may be funky gas dynamics
	// with regard to requested and actually given gas (2300 stipend, 63/64 rule).
	if t.descended {
		if depth >= t.currentDepth {
			t.currentGas = gas
			//t.traces[topIdx].Gas = gas
		} else {
			// TODO(karalabe): The call was made to a plain account. We currently don't
			// have access to the true gas amount inside the call and so any amount will
			// mostly be wrong since it depends on a lot of input args. Skip gas for now.
		}
		t.descended = false
	}
	if op == vm.REVERT {
		trace := mergeBase(Trace{
			ErrorString: "execution reverted",
		})
		t.traces = append(t.traces, trace)
		return
	}

	if depth == t.currentDepth-1 {
		trace := Trace{
			Type:  "CALLEND",
			GasIn: gas,
			Value: scope.Stack.Peek().Bytes(),
		}
		t.traces = append(t.traces, trace)
	}

	t.currentDepth = depth

	//call := t.traces[topIdx]
	//t.traces = t.traces[0:topIdx]
	//topIdx--

	//if call.op == vm.CREATE || call.op == vm.CREATE2 {
	//	if call.GasIn > 0 {
	//		call.GasUsed = call.GasIn - call.GasCost + gas // TODO double check this
	//	}
	//
	//	ret := scope.Stack.Peek()
	//	if ret.Uint64() != 0 {
	//		addr := libcommon.BytesToAddress(scope.Stack.Peek().Bytes())
	//		call.To = addr
	//		call.Output = t.env.IntraBlockState().GetCode(addr)
	//	} else if err == nil {
	//
	//		call.ErrorString = "internal failure" // TODO(karalabe): surface these faults somehow
	//	}
	//} else {
	//	call.GasUsed = call.GasIn - call.GasCost + call.Gas - gas
	//	ret := scope.Stack.Peek()
	//	if ret.Uint64() != 0 {
	//		call.Output = t.returnData
	//	} else if err == nil {
	//		call.ErrorString = "internal failure" // TODO(karalabe): surface these faults somehow
	//	}
	//
	//	// Inject the call into the previous one
	//	t.traces[topIdx].Calls = append(t.traces[topIdx].Calls, call)
	//	t.returnData = nil
	//}
}

func (t *sentioTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracer) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {}

func (t *sentioTracer) GetResult() (json.RawMessage, error) {
	return json.Marshal(t.rootTrace)
	//return json.RawMessage(`{}`), nil
}

func (t *sentioTracer) Stop(err error) {

}

//func (t *sentioTracer) Fault(err error) {
//	topIdx := len(t.traces) - 1
//	// If the topmost call already reverted, don't handle the additional fault again
//	if t.traces[topIdx].ErrorString != "" {
//		return
//	}
//
//	call := t.traces[topIdx]
//	t.traces = t.traces[0:topIdx]
//	topIdx--
//
//	// Consume all available gas and clean any leftovers
//	if call.Gas != 0 {
//		//call.gas = '0x' + bigInt(call.gas).toString(16)
//		call.GasUsed = call.Gas
//	}
//
//	if topIdx >= 0 {
//		t.traces[topIdx].Calls = append(t.traces[topIdx].Calls, call)
//		return
//	}
//	//// Last call failed too, leave it in the stack
//	t.traces = append(t.traces, call)
//}

func newSentioTracer(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	log.Warn("checking tracer name: ", name)

	if name != "sentio" {
		return nil, errors.New("no tracer found")
	}

	// TODO add configures
	return &sentioTracer{
		//internalCall: true,
	}, nil
}

func (t *sentioTracer) isPrecompiled(addr libcommon.Address) bool {
	for _, p := range t.activePrecompiles {
		if p == addr {
			return true
		}
	}
	return false
}
