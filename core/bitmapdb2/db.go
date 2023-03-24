package bitmapdb2

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/sroar"
	"github.com/ledgerwatch/log/v3"
)

var DefaultSliceSize = 4096

// BitmapDB2 is a database that stores bitmaps.
// We do this to offload data from mdbx, which does not scale well on modern hardware by preventing parallel writes.
// Internally, the database is organized similarly to bitmapdb, which contains Roaring bitmaps, sharded by a slice size (in bytes).
type DB struct {
	ldb       *pebble.DB
	sliceSize uint64
}

type Batch struct {
	mutex            sync.Mutex
	db               *DB
	batch            *pebble.Batch
	autoCommitTxSize int
}

func NewBitmapDB2(datadir string, sliceSize uint64) *DB {
	path := datadir + "/bitmapdb2"
	ldb, err := pebble.Open(path, &pebble.Options{
		MemTableSize: 16 * 1024 * 1024,
	})
	if err != nil {
		panic(err)
	}
	log.Info("BitmapDB2 open", "path", path, "sliceSize", sliceSize)
	return &DB{
		ldb:       ldb,
		sliceSize: sliceSize,
	}
}

func (db *DB) Close() {
	db.ldb.Close()
}

func (db *DB) NewBatch() *Batch {
	return &Batch{
		db:    db,
		batch: db.ldb.NewIndexedBatch(),
	}
}

func (db *DB) NewAutoBatch(txSize int) *Batch {
	return &Batch{
		db:               db,
		batch:            db.ldb.NewIndexedBatch(),
		autoCommitTxSize: txSize,
	}
}

func (db *DB) GetBitmap(bucket string, key []byte, from, to uint64) (*sroar.Bitmap, error) {
	prefix := bitmapRowPrefix(bucket, key)
	iter := db.ldb.NewIter(nil)
	defer iter.Close()

	var slices []*sroar.Bitmap
	for iter.SeekGE(bitmapRowKey(bucket, key, from)); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		slice := sroar.FromBufferWithCopy(iter.Value())
		slices = append(slices, slice)

		// If range of the current slice contains to, we can stop.
		// This is ensured by UpsertBitmap not writing slices that have overlapping ranges.
		if getSliceMaxFromRowKey(iter.Key()) >= to {
			break
		}
	}
	return sroar.FastOr(slices...), nil
}

func (b *Batch) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if err := b.batch.Close(); err != nil {
		return err
	}
	b.batch = nil
	return nil
}

func (b *Batch) Commit() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	return b.commitInternal()
}

func (b *Batch) TruncateBitmap(bucket string, key []byte, from uint64) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	prefix := bitmapRowPrefix(bucket, key)
	iter := b.batch.NewIter(nil)
	defer iter.Close()

	var slices []*sroar.Bitmap
	for iter.SeekGE(bitmapRowKey(bucket, key, from)); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		slice := sroar.FromBufferWithCopy(iter.Value())
		slices = append(slices, slice)

		if err := b.batch.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}

	value := sroar.FastOr(slices...)
	if !value.IsEmpty() && value.Maximum() >= from {
		value.RemoveRange(from, uint64(value.Maximum()+1))
	}
	if !value.IsEmpty() {
		if err := b.upsertBitmapInternal(bucket, key, value); err != nil {
			return err
		}
	}
	return b.autoCommit()
}

// Value may be modified in place.  Caller must not use the value after calling this method.
func (b *Batch) UpsertBitmap(bucket string, key []byte, value *sroar.Bitmap) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	return b.upsertBitmapInternal(bucket, key, value)
}

func (b *Batch) upsertBitmapInternal(bucket string, key []byte, value *sroar.Bitmap) error {
	if value.IsEmpty() {
		return nil
	}
	prefix := bitmapRowPrefix(bucket, key)
	iter := b.batch.NewIter(nil)
	defer iter.Close()

	var slices []*sroar.Bitmap
	slices = append(slices, value)
	for iter.SeekGE(bitmapRowKey(bucket, key, uint64(value.Minimum()))); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		slice := sroar.FromBufferWithCopy(iter.Value())
		slices = append(slices, slice)

		if err := b.batch.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}

	newValue := sroar.FastOr(slices...)
	externalSize := func(start, end uint64) uint64 { return 0 }
	newSlices := newValue.Split(externalSize, b.db.sliceSize)
	for idx, slice := range newSlices {
		var rowKey []byte
		isLast := idx == len(newSlices)-1
		if isLast {
			rowKey = bitmapRowKey(bucket, key, ^uint64(0))
		} else {
			rowKey = bitmapRowKey(bucket, key, uint64(slice.Maximum()))
		}
		if err := b.batch.Set(rowKey, slice.ToBuffer(), nil); err != nil {
			return err
		}
	}
	return b.autoCommit()
}

func (b *Batch) commitInternal() error {
	startTime := time.Now()
	txSize := b.batch.Len()
	defer func() {
		log.Info("Batch commit done", "time", time.Since(startTime), "txSize", txSize)
	}()
	return b.batch.Commit(nil)
}

func (b *Batch) autoCommit() error {
	if b.autoCommitTxSize == 0 {
		return nil
	}
	if b.autoCommitTxSize > 0 && b.autoCommitTxSize <= b.batch.Len() {
		if err := b.commitInternal(); err != nil {
			return err
		}
		b.batch = b.db.ldb.NewIndexedBatch()
	}
	return nil
}
