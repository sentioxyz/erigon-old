package geth

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbopt "github.com/syndtr/goleveldb/leveldb/opt"
)

// The list of table names of chain freezer.
const (
	// chainFreezerHeaderTable indicates the name of the freezer header table.
	chainFreezerHeaderTable = "headers"

	// chainFreezerHashTable indicates the name of the freezer canonical hash table.
	chainFreezerHashTable = "hashes"

	// chainFreezerBodiesTable indicates the name of the freezer block body table.
	chainFreezerBodiesTable = "bodies"

	// chainFreezerReceiptTable indicates the name of the freezer receipts table.
	chainFreezerReceiptTable = "receipts"

	// chainFreezerDifficultyTable indicates the name of the freezer total difficulty table.
	chainFreezerDifficultyTable = "diffs"
)

type DB struct {
	io.Closer
	ldb           *leveldb.DB
	ancientRoot   string
	ancientTables map[string]*FreezerTable
}

func (db *DB) Close() error {
	return db.ldb.Close()
}

func NewDB(chaindata string) *DB {
	opts := &leveldbopt.Options{
		ReadOnly: true,
	}
	ldb, err := leveldb.OpenFile(chaindata, opts)
	if err != nil {
		panic(err)
	}

	db := &DB{
		ldb:           ldb,
		ancientRoot:   fmt.Sprintf("%s/ancient", chaindata),
		ancientTables: make(map[string]*FreezerTable),
	}
	tablesToOpen := [][]any{
		{chainFreezerHeaderTable, false},
		{chainFreezerHashTable, true},
		{chainFreezerBodiesTable, false},
		{chainFreezerReceiptTable, false},
	}

	for _, p := range tablesToOpen {
		name := p[0].(string)
		noCompression := p[1].(bool)
		table, err := NewFreezerTable(db.ancientRoot, name, noCompression)
		if err != nil {
			panic(err)
		}
		db.ancientTables[name] = table
		log.Info("Opened ancient table", "name", name, "numItems", table.items)
	}
	return db
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.ldb.Get(key, nil)
}

func (db *DB) Ancient(name string, number uint64) ([]byte, error) {
	table, ok := db.ancientTables[name]
	if !ok {
		return nil, fmt.Errorf("unknown ancient table %s", name)
	}
	return table.Retrieve(number)
}

// isCanon is an internal utility method, to check whether the given number/hash
// is part of the ancient (canon) set.
func (db *DB) IsCanon(number uint64, hash common.Hash) bool {
	h, err := db.Ancient("hashes", number)
	if err != nil {
		return false
	}
	return bytes.Equal(h, hash[:])
}
