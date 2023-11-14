package mev

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
)

type simulateEnv struct {
	block   *types.Block
	header  *types.Header
	statedb *state.IntraBlockState
	tracer  *SimulateTracer
	ec      SimulateContext
}

func (s *InfraServer) withSimulateEnv(ctx context.Context,
	blockNumber uint64, abiType string,
	tracer *SimulateTracer,
	prepFn func(*simulateEnv) error,
	executeFn func(*simulateEnv) error) error {
	tx, err := s.eth.ChainDB().BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	block, err := s.GetBlockByNumber(ctx,
		rpc.BlockNumber(blockNumber))
	if err != nil {
		return err
	}
	if block == nil {
		return errors.New("block re-org detected")
	}
	env := &simulateEnv{
		tracer: tracer,
		block:  block,
		header: &types.Header{
			ParentHash: block.Hash(),
			Number:     new(big.Int).Add(block.Number(), big.NewInt(1)),
			GasLimit:   block.GasLimit(),
			Time:       uint64(time.Now().Unix()),
			Difficulty: block.Difficulty(),
			Coinbase:   block.Coinbase(),
			BaseFee:    block.BaseFee(),
		},
	}
	if err := prepFn(env); err != nil {
		return err
	}
	ec, err := CreateSimulationContext(ctx, s.eth, tx, abiType, env.statedb, env.header, tracer)
	if err != nil {
		return fmt.Errorf("failed to create execution context: %w", err)
	}
	env.ec = ec
	return executeFn(env)
}
