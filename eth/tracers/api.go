package tracers

import (
	"encoding/json"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

// TraceConfig holds extra parameters to trace functions.
type TraceConfig struct {
	*logger.LogConfig
	Tracer         *string
	TracerConfig   *json.RawMessage
	Timeout        *string
	Reexec         *uint64
	NoRefunds      *bool // Turns off gas refunds when tracing
	StateOverrides *ethapi.StateOverrides

	IgnoreGas             *bool
	IgnoreCodeSizeLimit   *bool
	CreationCodeOverrides map[libcommon.Address]hexutility.Bytes
	CreateAddressOverride *libcommon.Address

	BorTraceEnabled *bool
	BorTx           *bool
}
