package mev

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/log/v3"
)

type simulateEnv struct {
	header  *types.Header
	statedb *state.IntraBlockState
	tracer  *SimulateTracer
	ec      SimulateContext
}

func (s *InfraServer) StateDBAtBlock(tx kv.Tx,
	blockNumber uint64,
) (*state.IntraBlockState, error) {
	stateCache := shards.NewStateCache(32, 0)
	stateReader, err := rpchelper.CreateHistoryStateReader(
		tx, blockNumber,
		0, false,
		s.eth.ChainConfig().ChainName,
		s.eth.BitmapDB())
	if err != nil {
		return nil, err
	}
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	ibs := state.New(cachedReader)
	return ibs, nil
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
	header, err := s.erigonBackend.GetHeaderByNumber(ctx, rpc.BlockNumber(blockNumber))
	if err != nil {
		log.Error("failed to get header by number")
		return err
	}
	parentBN := blockNumber - 1

	_, err = s.erigonBackend.GetHeaderByNumber(ctx, rpc.BlockNumber(parentBN))
	if err != nil {
		log.Error("failed to get parent header by number")
		return err
	}
	statedb, err := s.StateDBAtBlock(tx, parentBN)
	if err != nil {
		log.Error("failed to create state db")
		return errors.New("failed to create state db")
	}
	statedb.Reset()

	env := &simulateEnv{
		tracer:  tracer,
		statedb: statedb,
		header:  header,
	}
	header.Time = uint64(time.Now().Unix())
	if err := prepFn(env); err != nil {
		log.Error("prepare fn failed")
		return err
	}
	ec, err := CreateSimulationContext(ctx, s.eth, s.erigonBackend, abiType, env.statedb, env.header, env.tracer)
	if err != nil {
		return fmt.Errorf("failed to create execution context: %w", err)
	}
	env.ec = ec
	return executeFn(env)
}
