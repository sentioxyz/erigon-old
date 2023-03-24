package bitmapdb2

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/erigon-lib/common"
)

type parallelLoadTask struct {
	commit bool
	bucket string
	key    []byte
	value  *roaring.Bitmap
}

type parallelLoadWork struct {
	batch  *Batch
	ch     chan *parallelLoadTask
	doneCh chan error
	errCh  chan error

	summary ParallelLoadSummary
}

type ParallelLoadSummary struct {
	TotalTime             time.Duration
	NumPuts               uint64
	TotalValueCardinality uint64
}

type ParallelLoader struct {
	shards  []*parallelLoadWork
	Summary *ParallelLoadSummary
}

var DefaultChannelBufferSize = 1024

func NewParallelLoader(db *DB, numShards int, batchTxSize int, channelBufferSize int) *ParallelLoader {
	shards := make([]*parallelLoadWork, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &parallelLoadWork{
			batch:  db.NewAutoBatch(batchTxSize),
			ch:     make(chan *parallelLoadTask, channelBufferSize),
			doneCh: make(chan error, 1),
			errCh:  make(chan error, 1),
		}
	}
	l := &ParallelLoader{
		shards: shards,
	}
	for i := 0; i < numShards; i++ {
		go l.shardWork(i)
	}
	return l
}

func (l *ParallelLoader) computeShard(key []byte) int {
	if len(key) < 8 {
		key = append(make([]byte, 8-len(key)), key...)
	}
	idx := binary.BigEndian.Uint64(key[len(key)-8:]) % uint64(len(l.shards))
	return int(idx)
}

// Loads a bitmap into the database.
// Parameters can be modified after the call returns.
func (l *ParallelLoader) Load(bucket string, key []byte, value *roaring.Bitmap) error {
	// Make sure new value refers to no byte buffers of the old value.
	valueCopy := value.Clone()
	valueCopy.CloneCopyOnWriteContainers()

	task := &parallelLoadTask{false, bucket, common.Copy(key), valueCopy}
	shard := l.shards[l.computeShard(key)]
	select {
	default:
	case err := <-shard.errCh:
		return err
	}
	shard.ch <- task
	return nil
}

func (l *ParallelLoader) shardWork(shardIdx int) {
	shard := l.shards[shardIdx]
	var lastErr error
	for task := range shard.ch {
		startTime := time.Now()
		batch := shard.batch
		if batch == nil {
			return
		}
		if task.commit {
			// Commit only if there were no errors.
			if lastErr != nil {
				shard.doneCh <- lastErr
			} else {
				shard.doneCh <- batch.Commit()
				shard.summary.TotalTime += time.Since(startTime)
			}
			return
		} else if lastErr != nil {
			// If there was an error, ignore all the following tasks.
			continue
		}
		shard.summary.NumPuts++
		shard.summary.TotalValueCardinality += uint64(task.value.GetCardinality())
		if err := batch.UpsertBitmap(task.bucket, task.key, task.value); err != nil {
			lastErr = err
			shard.errCh <- err
		}
		shard.summary.TotalTime += time.Since(startTime)
	}
}

func (l *ParallelLoader) Close() error {
	for _, shard := range l.shards {
		if shard.batch == nil {
			continue
		}
		shard.batch.Close()
		shard.batch = nil
	}
	return nil
}

func (l *ParallelLoader) Commit() error {
	var errs []error
	l.Summary = &ParallelLoadSummary{}
	for idx, shard := range l.shards {
		if shard.batch == nil {
			return fmt.Errorf("shard %d is already closed", idx)
		}
		shard.ch <- &parallelLoadTask{commit: true}
		close(shard.ch)
		if err := <-shard.doneCh; err != nil {
			errs = append(errs, err)
		}
		l.Summary.Add(shard.summary)
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing shards: %v", errs)
	}
	return nil
}

func (s *ParallelLoadSummary) Add(other ParallelLoadSummary) {
	s.TotalTime += other.TotalTime
	s.NumPuts += other.NumPuts
	s.TotalValueCardinality += other.TotalValueCardinality
}

func (s *ParallelLoadSummary) String() string {
	return fmt.Sprintf("total time: %s, num puts: %d, total value cardinality: %d", s.TotalTime, s.NumPuts, s.TotalValueCardinality)
}
