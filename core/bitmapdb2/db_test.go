package bitmapdb2

import (
	"math/rand"
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestUpsertBitmap(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir, 64)
	defer db.Close()

	seed := int64(497777)
	r := rand.New(rand.NewSource(seed))

	truthBitmaps := make([]*roaring.Bitmap, 16)
	for i := 0; i < 16; i++ {
		truthBitmaps[i] = roaring.New()
	}

	batch := db.NewBatch()
	for i := 0; i < 500; i++ {
		bitmap := roaring.New()
		for j := 0; j < 100; j++ {
			bitmap.Add(uint32(r.Int31n(1_000_000)))
		}
		removeFrom := 1_000_000 - 1000*uint64(r.Int63n(100))
		batch.TruncateBitmap("test", []byte{byte(i % 16)}, removeFrom)
		truth := truthBitmaps[i%16]
		if !truth.IsEmpty() {
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
		if !m.Equals(truthBitmaps[i]) {
			t.Fatalf("bitmap not match, %d not found, values: %v", i, m.ToArray())
		}
	}
}
