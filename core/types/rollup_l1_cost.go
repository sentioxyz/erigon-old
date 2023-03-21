package types

import (
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
)

type RollupGasData struct {
	Zeroes, Ones uint64
}

func (r RollupGasData) DataGas(time uint64, cfg *chain.Config) (gas uint64) {
	gas = r.Zeroes * fixedgas.TxDataZeroGas
	if cfg.IsRegolith(time) {
		gas += r.Ones * fixedgas.TxDataNonZeroGasEIP2028
	} else {
		gas += (r.Ones + 68) * fixedgas.TxDataNonZeroGasEIP2028
	}
	return gas
}
