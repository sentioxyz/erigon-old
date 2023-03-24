package bitmapdb2

import (
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/dgraph-io/sroar"
)

func TestUpsertBitmap(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir, 512)
	defer db.Close()

	seed := int64(497777)
	r := rand.New(rand.NewSource(seed))

	truthBitmaps := make([]*sroar.Bitmap, 16)
	for i := 0; i < 16; i++ {
		truthBitmaps[i] = sroar.NewBitmap()
	}

	batch := db.NewBatch()
	for i := 0; i < 500; i++ {
		bitmap := sroar.NewBitmap()
		for j := 0; j < 100; j++ {
			bitmap.Set(uint64(r.Int31n(1_000_000)))
		}
		removeFrom := 1_000_000 - 1000*uint64(r.Int63n(100))
		batch.TruncateBitmap("test", []byte{byte(i % 16)}, removeFrom)
		truth := truthBitmaps[i%16]
		if !truth.IsEmpty() && uint64(truth.Maximum()) >= removeFrom {
			truth.RemoveRange(removeFrom, uint64(truth.Maximum())+1)
		}
		batch.UpsertBitmap("test", []byte{byte(i % 16)}, bitmap)
		truth.Or(bitmap)
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 16; i++ {
		m, err := db.GetBitmap("test", []byte{byte(i)}, 0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		b1 := truthBitmaps[i].ToArray()
		b2 := m.ToArray()
		if !reflect.DeepEqual(b1, b2) {
			t.Fatalf("bitmap not match, expected: %v, actual: %v", b1, b2)
		}
	}
}

func TestParallelLoad(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir, uint64(DefaultSliceSize))
	defer db.Close()

	seed := int64(6666666)
	r := rand.New(rand.NewSource(seed))

	truthBitmaps := make([]*sroar.Bitmap, 16)
	for i := 0; i < 16; i++ {
		truthBitmaps[i] = sroar.NewBitmap()
	}

	pl := NewParallelLoader(db, 7, 4096, 16)
	defer pl.Close()
	for i := 0; i < 1000; i++ {
		bitmap := sroar.NewBitmap()
		for j := 0; j < 100; j++ {
			bitmap.Set(uint64(r.Int31n(1_000_000)))
		}
		truth := truthBitmaps[i%16]
		pl.Load("test", []byte{byte(i % 16)}, bitmap)
		truth.Or(bitmap)
	}
	if err := pl.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 16; i++ {
		m, err := db.GetBitmap("test", []byte{byte(i)}, 0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		b1 := truthBitmaps[i].ToArray()
		b2 := m.ToArray()
		if !reflect.DeepEqual(b1, b2) {
			t.Fatalf("bitmap not match, expected: %v, actual: %v", b1, b2)
		}
	}
}
