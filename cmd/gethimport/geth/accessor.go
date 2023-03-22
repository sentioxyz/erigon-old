package geth

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	headBlockKey          = []byte("LastBlock")
	headerPrefix          = []byte("h")           // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix        = []byte("t")           // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix      = []byte("n")           // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix    = []byte("H")           // headerNumberPrefix + hash -> num (uint64 big endian)
	blockBodyPrefix       = []byte("b")           // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	CodePrefix            = []byte("c")           // CodePrefix + code hash -> account code
	blockReceiptsPrefix   = []byte("r")           // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts
	trieNodeAccountPrefix = []byte("A")           // trieNodeAccountPrefix + hexPath -> trie node
	trieNodeStoragePrefix = []byte("O")           // trieNodeStoragePrefix + accountHash + hexPath -> trie node
	PreimagePrefix        = []byte("secure-key-") // PreimagePrefix + hash -> preimage
)

// encodeBlockNumber encodes a block number as big endian uint64
func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

// headerNumberKey = headerNumberPrefix + hash
func headerNumberKey(hash common.Hash) []byte {
	return append(headerNumberPrefix, hash.Bytes()...)
}

// headerHashKey = headerPrefix + num (uint64 big endian) + headerHashSuffix
func headerHashKey(number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), headerHashSuffix...)
}

// headerKey = headerPrefix + num (uint64 big endian) + hash
func headerKey(number uint64, hash common.Hash) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

// blockBodyKey = blockBodyPrefix + num (uint64 big endian) + hash
func blockBodyKey(number uint64, hash common.Hash) []byte {
	return append(append(blockBodyPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

// blockReceiptsKey = blockReceiptsPrefix + num (uint64 big endian) + hash
func blockReceiptsKey(number uint64, hash common.Hash) []byte {
	return append(append(blockReceiptsPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

// preimageKey = PreimagePrefix + hash
func preimageKey(hash common.Hash) []byte {
	return append(PreimagePrefix, hash.Bytes()...)
}

// accountTrieNodeKey = trieNodeAccountPrefix + nodePath.
func accountTrieNodeKey(path []byte) []byte {
	return append(trieNodeAccountPrefix, path...)
}

// codeKey = CodePrefix + hash
func codeKey(hash common.Hash) []byte {
	return append(CodePrefix, hash.Bytes()...)
}

// headerTDKey = headerPrefix + num (uint64 big endian) + hash + headerTDSuffix
func headerTDKey(number uint64, hash common.Hash) []byte {
	return append(headerKey(number, hash), headerTDSuffix...)
}

func (db *DB) ReadCanonicalHash(number uint64) common.Hash {
	data, _ := db.Get(headerHashKey(number))
	return common.BytesToHash(data)
}

// ReadHeadBlockHash retrieves the hash of the current canonical head block.
func (db *DB) ReadHeadBlockHash() common.Hash {
	data, _ := db.Get(headBlockKey)
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// ReadHeaderNumber returns the header number assigned to a hash.
func (db *DB) ReadHeaderNumber(hash common.Hash) *uint64 {
	data, _ := db.Get(headerNumberKey(hash))
	if len(data) != 8 {
		return nil
	}
	number := binary.BigEndian.Uint64(data)
	return &number
}

func (db *DB) ReadAncientHeaderHash(number uint64) (common.Hash, error) {
	data, _ := db.Ancient("headers", number)
	if len(data) == 0 {
		return common.Hash{}, nil
	}
	return crypto.Keccak256Hash(data), nil
}

// ReadHeader retrieves the block header corresponding to the hash.
func (db *DB) ReadHeader(hash common.Hash, number uint64) (*types.Header, error) {
	data, _ := db.Ancient("headers", number)
	if len(data) == 0 || crypto.Keccak256Hash(data) != hash {
		data, _ = db.Get(headerKey(number, hash))
	}
	if len(data) == 0 {
		return nil, nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		log.Error("Invalid block header RLP", "hash", hash, "err", err)
		return nil, err
	}
	return header, nil
}

// ReadBody retrieves the block body corresponding to the hash.
func (db *DB) ReadBody(hash common.Hash, number uint64) (*types.Body, error) {
	var data []byte
	if db.IsCanon(number, hash) {
		data, _ = db.Ancient(chainFreezerBodiesTable, number)
	} else {
		data, _ = db.Get(blockBodyKey(number, hash))
	}
	if len(data) == 0 {
		return nil, nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil, err
	}
	return body, nil
}

var (
	receiptStatusFailedRLP     = []byte{}
	receiptStatusSuccessfulRLP = []byte{0x01}
)

const (
	// ReceiptStatusFailed is the status code of a transaction if execution failed.
	ReceiptStatusFailed = uint64(0)

	// ReceiptStatusSuccessful is the status code of a transaction if execution succeeded.
	ReceiptStatusSuccessful = uint64(1)
)

// ReceiptForStorage is a wrapper around a Receipt that flattens and parses the
// entire content of a receipt, as opposed to only the consensus fields originally.
type ReceiptForStorageGeth types.Receipt

func (r *ReceiptForStorageGeth) DecodeRLP(s *rlp.Stream) error {
	var stored struct {
		PostStateOrStatus []byte
		CumulativeGasUsed uint64
		Logs              []*types.LogForStorage
		L1GasUsed         *big.Int
		L1GasPrice        *big.Int
		L1Fee             *big.Int
		FeeScalar         string
	}
	if err := s.Decode(&stored); err != nil {
		return err
	}
	if err := (*types.Receipt)(r).SetStatus(stored.PostStateOrStatus); err != nil {
		return err
	}
	r.CumulativeGasUsed = stored.CumulativeGasUsed
	r.Logs = make([]*types.Log, len(stored.Logs))
	for i, log := range stored.Logs {
		r.Logs[i] = (*types.Log)(log)
	}
	return nil
}

// ReadRawReceipts retrieves all the transaction receipts belonging to a block.
// The receipt metadata fields are not guaranteed to be populated, so they
// should not be used. Use ReadReceipts instead if the metadata is needed.
func (db *DB) ReadRawReceipts(hash common.Hash, number uint64) (types.Receipts, error) {
	// Retrieve the flattened receipt slice
	var data []byte
	if db.IsCanon(number, hash) {
		data, _ = db.Ancient(chainFreezerReceiptTable, number)
	} else {
		data, _ = db.Get(blockReceiptsKey(number, hash))
	}
	if len(data) == 0 {
		return nil, nil
	}
	// Convert the receipts from their storage form to their internal representation
	storageReceipts := []*ReceiptForStorageGeth{}
	if err := rlp.DecodeBytes(data, &storageReceipts); err != nil {
		log.Error("Invalid receipt array RLP", "hash", hash, "err", err, "data", hex.EncodeToString(data))
		return nil, err
	}
	receipts := make(types.Receipts, len(storageReceipts))
	for i, storageReceipt := range storageReceipts {
		receipts[i] = (*types.Receipt)(storageReceipt)
	}
	return receipts, nil
}

// ReadBlock retrieves an entire block corresponding to the hash, assembling it
// back from the stored header and body. If either the header or body could not
// be retrieved nil is returned.
func (db *DB) ReadBlock(hash common.Hash, number uint64) (*types.Block, error) {
	header, err := db.ReadHeader(hash, number)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	body, err := db.ReadBody(hash, number)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}

	return types.NewBlockFromStorage(header.Hash(), header,
		body.Transactions, body.Uncles, body.Withdrawals), nil
}

func (db *DB) GetTrieNode(hash []byte) (*TrieNode, error) {
	data, _ := db.Get(hash)
	if len(data) == 0 {
		return nil, nil
	}

	return decodeNode(hash, data)
}

func (db *DB) ForEachPreimage(fn func(hash common.Hash, preimage []byte) error) error {
	iter := db.ldb.NewIterator(util.BytesPrefix(PreimagePrefix), nil)
	defer iter.Release()
	for iter.Next() {
		value := iter.Value()
		hash := common.BytesToHash(iter.Key()[len(PreimagePrefix):])
		if err := fn(hash, value); err != nil {
			return err
		}
	}
	return iter.Error() // unroll any accumulated error
}

// ReadCode retrieves the contract code of the provided code hash.
func (db *DB) ReadCode(hash common.Hash) []byte {
	// Try with the prefixed code scheme first, if not then try with legacy
	// scheme.
	data := db.ReadCodeWithPrefix(hash)
	if len(data) != 0 {
		return data
	}
	data, _ = db.Get(hash.Bytes())
	return data
}

// ReadCodeWithPrefix retrieves the contract code of the provided code hash.
// The main difference between this function and ReadCode is this function
// will only check the existence with latest scheme(with prefix).
func (db *DB) ReadCodeWithPrefix(hash common.Hash) []byte {
	data, _ := db.Get(codeKey(hash))
	return data
}

// ReadTdRLP retrieves a block's total difficulty corresponding to the hash in RLP encoding.
func (db *DB) ReadTdRLP(hash common.Hash, number uint64) rlp.RawValue {
	// First try to look up the data in ancient database. Extra hash
	// comparison is necessary since ancient database only maintains
	// the canonical data.
	data, _ := db.Ancient(chainFreezerDifficultyTable, number)
	if len(data) > 0 {
		h, _ := db.Ancient(chainFreezerHashTable, number)
		if common.BytesToHash(h) == hash {
			return data
		}
	}
	// Then try to look up the data in leveldb.
	data, _ = db.Get(headerTDKey(number, hash))
	if len(data) > 0 {
		return data
	}
	return nil // Can't find the data anywhere.
}

// ReadTd retrieves a block's total difficulty corresponding to the hash.
func (db *DB) ReadTd(hash common.Hash, number uint64) *big.Int {
	data := db.ReadTdRLP(hash, number)
	if len(data) == 0 {
		return nil
	}
	td := new(big.Int)
	if err := rlp.Decode(bytes.NewReader(data), td); err != nil {
		log.Error("Invalid block total difficulty RLP", "hash", hash, "err", err)
		return nil
	}
	return td
}

type StateAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}

func (a *StateAccount) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	if a.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read Balance: %w", err)
	}
	a.Balance = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Root: %w", err)
	}
	a.Root = common.BytesToHash(b)
	if a.CodeHash, err = s.Bytes(); err != nil {
		return fmt.Errorf("read CodeHash: %w", err)
	}
	return nil
}
