package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/bitmapdb2"
	"github.com/ledgerwatch/erigon/ethdb"
)

type iter interface {
	IsHistory() bool

	Initialize(tx kv.Tx, bitmapDB2 *bitmapdb2.DB,
		startAddress libcommon.Address, startLocation libcommon.Hash,
		startBlock uint64) error

	Next() (bool, error)
	CurrentAddr() []byte
	CurrentLoc() []byte
	SeekGTE(n uint64) (uint64, bool, error)

	Close()
}

type hIter struct {
	shCursor kv.Cursor
	hCursor  *ethdb.SplitCursor
	initSeek bool

	startBlock             uint64
	startAddress           libcommon.Address
	hAddr, hLoc, tsEnc, hV []byte
}

func (h *hIter) IsHistory() bool {
	return true
}

func (h *hIter) Close() {
	h.shCursor.Close()
}

func (h *hIter) Initialize(tx kv.Tx, bitmapDB2 *bitmapdb2.DB,
	startAddress libcommon.Address, startLocation libcommon.Hash,
	startBlock uint64) error {
	var startkeyNoInc = make([]byte, length.Addr+length.Hash)
	copy(startkeyNoInc, startAddress.Bytes())
	copy(startkeyNoInc[length.Addr:], startLocation.Bytes())

	shCursor, err := tx.Cursor(kv.StorageHistory)
	if err != nil {
		return err
	}
	var hCursor = ethdb.NewSplitCursor(
		shCursor,
		startkeyNoInc,
		8*length.Addr,
		length.Addr,             /* part1end */
		length.Addr,             /* part2start */
		length.Addr+length.Hash, /* part3start */
	)
	h.shCursor = shCursor
	h.hCursor = hCursor
	h.startAddress = startAddress
	h.startBlock = startBlock
	return nil
}

func (h *hIter) Next() (bool, error) {
	var err error
	if !h.initSeek {
		h.hAddr, h.hLoc, h.tsEnc, h.hV, err = h.hCursor.Seek()
		h.initSeek = true
	} else {
		h.hAddr, h.hLoc, h.tsEnc, h.hV, err = h.hCursor.Next()
	}
	if err != nil {
		return false, err
	}
	for bytes.Compare(h.hAddr, h.startAddress[:]) == 0 && h.hLoc != nil && binary.BigEndian.Uint64(h.tsEnc) < h.startBlock {
		// Forward to the desired block.
		if h.hAddr, h.hLoc, h.tsEnc, h.hV, err = h.hCursor.Next(); err != nil {
			return false, err
		}
	}
	return bytes.Compare(h.hAddr, h.startAddress[:]) == 0 && h.hLoc != nil, nil
}

func (h *hIter) CurrentAddr() []byte {
	return h.hAddr
}

func (h *hIter) CurrentLoc() []byte {
	return h.hLoc
}

func (h *hIter) SeekGTE(n uint64) (uint64, bool, error) {
	index := roaring64.New()
	if _, err := index.ReadFrom(bytes.NewReader(h.hV)); err != nil {
		return 0, false, err
	}
	found, ok := bitmapdb.SeekInBitmap64(index, n)
	return found, ok, nil
}

type db2Iter struct {
	db           *bitmapdb2.DB
	bucket       string
	it           *bitmapdb2.DBIterator
	startBlock   uint64
	startAddress libcommon.Address
}

func (d *db2Iter) IsHistory() bool {
	return true
}

func (d *db2Iter) Close() {
	d.it.Close()
}

func (d *db2Iter) Initialize(tx kv.Tx, bitmapDB2 *bitmapdb2.DB,
	startAddress libcommon.Address, startLocation libcommon.Hash,
	startBlock uint64) error {
	startKey := make([]byte, length.Addr+length.Hash)
	copy(startKey, startAddress.Bytes())
	copy(startKey[length.Addr:], startLocation.Bytes())
	d.db = bitmapDB2
	d.bucket = kv.StorageHistory
	d.startBlock = startBlock
	d.startAddress = startAddress
	d.it = bitmapDB2.NewIterator(d.bucket, startKey)
	return nil
}

func (d *db2Iter) Next() (bool, error) {
	if !d.it.Next() {
		return false, nil
	}
	if !bytes.Equal(d.it.Key()[:length.Addr], d.startAddress[:]) {
		return false, nil
	}
	for d.it.MaxBlock() < d.startBlock {
		if !d.it.Next() {
			return false, nil
		}
	}
	return true, nil
}

func (d *db2Iter) CurrentAddr() []byte {
	return d.it.Key()[:length.Addr]
}

func (d *db2Iter) CurrentLoc() []byte {
	return d.it.Key()[length.Addr:]
}

func (d *db2Iter) SeekGTE(n uint64) (uint64, bool, error) {
	found, err := d.db.SeekFirstGTE(d.bucket, d.it.Key(), uint32(n))
	if err != nil {
		return 0, false, err
	}
	if found == 0 {
		return 0, false, nil
	}
	return uint64(found), true, nil
}

// multiWayMerger combines multiple iterators into one, returning the value with the smallest storage key.
type multiWayMerger struct {
	iters        []iter
	iterHasValue []bool
	currValueIt  int
}

func (m *multiWayMerger) Close() {
	for _, it := range m.iters {
		it.Close()
	}
}

func (m *multiWayMerger) Initialize(tx kv.Tx, bitmapDB2 *bitmapdb2.DB,
	startAddress libcommon.Address, startLocation libcommon.Hash,
	startBlock uint64) error {
	m.currValueIt = -1
	m.iterHasValue = make([]bool, len(m.iters))
	for _, it := range m.iters {
		if err := it.Initialize(tx, bitmapDB2, startAddress, startLocation, startBlock); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiWayMerger) Next() (bool, error) {
	var err error
	if m.currValueIt == -1 {
		for i, it := range m.iters {
			if m.iterHasValue[i], err = it.Next(); err != nil {
				return false, err
			}
		}
	} else {
		if m.iterHasValue[m.currValueIt], err = m.iters[m.currValueIt].Next(); err != nil {
			return false, err
		}
		m.currValueIt = -1
	}
	for i, hasValue := range m.iterHasValue {
		if !hasValue {
			continue
		}
		if m.currValueIt == -1 || bytes.Compare(m.iters[i].CurrentLoc(), m.iters[m.currValueIt].CurrentLoc()) < 0 {
			m.currValueIt = i
		}
	}
	return m.currValueIt != -1, nil
}

func (m *multiWayMerger) CurrentAddr() []byte {
	return m.iters[m.currValueIt].CurrentAddr()
}

func (m *multiWayMerger) CurrentLoc() []byte {
	return m.iters[m.currValueIt].CurrentLoc()
}

func (m *multiWayMerger) SeekGTE(n uint64) (uint64, bool, error) {
	hLoc := m.iters[m.currValueIt].CurrentLoc()
	minFound, ok, err := m.iters[m.currValueIt].SeekGTE(n)
	if err != nil {
		return 0, false, err
	}
	for i, it := range m.iters {
		if !m.iterHasValue[i] || !bytes.Equal(hLoc, it.CurrentLoc()) {
			continue
		}
		found, ok2, err := it.SeekGTE(n)
		if err != nil {
			return 0, false, err
		}
		if !ok2 {
			continue
		}
		if !ok || minFound > found {
			minFound = found
			ok = true
			m.currValueIt = i
		}
	}
	return minFound, ok, nil
}

// startKey is the concatenation of address and incarnation (BigEndian 8 byte)
func WalkAsOfStorage(tx kv.Tx, address libcommon.Address, incarnation uint64, startLocation libcommon.Hash, timestamp uint64,
	bitmapDB2 *bitmapdb2.DB,
	walker func(k1, k2, v []byte) (bool, error)) error {
	var startkey = make([]byte, length.Addr+length.Incarnation+length.Hash)
	copy(startkey, address.Bytes())
	binary.BigEndian.PutUint64(startkey[length.Addr:], incarnation)
	copy(startkey[length.Addr+length.Incarnation:], startLocation.Bytes())

	//for storage
	mCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer mCursor.Close()
	mainCursor := ethdb.NewSplitCursor(
		mCursor,
		startkey,
		8*(length.Addr+length.Incarnation),
		length.Addr,                    /* part1end */
		length.Addr+length.Incarnation, /* part2start */
		length.Addr+length.Incarnation+length.Hash, /* part3start */
	)

	hIt := &multiWayMerger{}
	hIt.iters = append(hIt.iters, &hIter{})
	if bitmapDB2 != nil {
		hIt.iters = append(hIt.iters, &db2Iter{})
	}
	if err := hIt.Initialize(tx, bitmapDB2, address, startLocation, timestamp); err != nil {
		return err
	}
	defer hIt.Close()
	moreHistory, err := hIt.Next()
	if err != nil {
		return err
	}

	csCursor, err := tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return err
	}
	defer csCursor.Close()

	addr, loc, _, v, err1 := mainCursor.Seek()
	if err1 != nil {
		return err1
	}

	goOn := true
	for goOn {
		cmp := -1
		// If history has exhausted, no comparasion is needed.
		if moreHistory {
			cmp = bytes.Compare(loc, hIt.CurrentLoc())
		}
		shouldForwardHistory, shouldForwardMain := false, false
		if !moreHistory || cmp < 0 {
			shouldForwardMain = true
			// No more history or history does not even contain the storage key - use the latest value.
			goOn, err = walker(addr, loc, v)
		} else if bytes.Compare(loc, hIt.CurrentLoc()) >= 0 {
			shouldForwardHistory = true
			// History contains the current storage key, or another key that is not present in the latest state.
			changeSetBlock, ok, err := hIt.SeekGTE(timestamp)
			if err != nil {
				return err
			}
			if ok {
				// Extract value from the changeSet
				csKey := make([]byte, 8+length.Addr+length.Incarnation)
				copy(csKey, dbutils.EncodeBlockNumber(changeSetBlock))
				copy(csKey[8:], address[:]) // address + incarnation
				binary.BigEndian.PutUint64(csKey[8+length.Addr:], incarnation)
				kData := csKey
				data, err3 := csCursor.SeekBothRange(csKey, hIt.CurrentLoc())
				if err3 != nil {
					return err3
				}
				if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hIt.CurrentLoc()) {
					return fmt.Errorf("inconsistent storage changeset and history kData %x, csKey %x, data %x, hLoc %x",
						kData, csKey, data, hIt.CurrentLoc())
				}
				data = data[length.Hash:]
				if len(data) > 0 { // Skip deleted entries
					goOn, err = walker(hIt.CurrentAddr(), hIt.CurrentLoc(), data)
				}
			} else if cmp == 0 {
				// No change to the value in the latest state.
				goOn, err = walker(addr, loc, v)
			}
		}
		if err != nil {
			return err
		}
		if goOn {
			if shouldForwardMain {
				if addr, loc, _, v, err1 = mainCursor.Next(); err1 != nil {
					return err1
				}
			}
			if shouldForwardHistory {
				hLoc0 := hIt.CurrentLoc()
				for moreHistory && bytes.Equal(hLoc0, hIt.CurrentLoc()) {
					if moreHistory, err = hIt.Next(); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func WalkAsOfAccounts(tx kv.Tx, startAddress libcommon.Address, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	mainCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer mainCursor.Close()
	ahCursor, err := tx.Cursor(kv.AccountsHistory)
	if err != nil {
		return err
	}
	defer ahCursor.Close()
	var hCursor = ethdb.NewSplitCursor(
		ahCursor,
		startAddress.Bytes(),
		0,             /* fixedBits */
		length.Addr,   /* part1end */
		length.Addr,   /* part2start */
		length.Addr+8, /* part3start */
	)
	csCursor, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return err
	}
	defer csCursor.Close()

	k, v, err1 := mainCursor.Seek(startAddress.Bytes())
	if err1 != nil {
		return err1
	}
	for k != nil && len(k) > length.Addr {
		k, v, err1 = mainCursor.Next()
		if err1 != nil {
			return err1
		}
	}
	hK, tsEnc, _, hV, err2 := hCursor.Seek()
	if err2 != nil {
		return err2
	}
	for hK != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
		hK, tsEnc, _, hV, err2 = hCursor.Next()
		if err2 != nil {
			return err2
		}
	}

	goOn := true
	for goOn {
		//exit or next conditions
		cmp, br := common.KeyCmp(k, hK)
		if br {
			break
		}
		if cmp < 0 {
			goOn, err = walker(k, v)
		} else {
			index := roaring64.New()
			_, err = index.ReadFrom(bytes.NewReader(hV))
			if err != nil {
				return err
			}
			found, ok := bitmapdb.SeekInBitmap64(index, timestamp)
			changeSetBlock := found
			if ok {
				// Extract value from the changeSet
				csKey := dbutils.EncodeBlockNumber(changeSetBlock)
				kData := csKey
				data, err3 := csCursor.SeekBothRange(csKey, hK)
				if err3 != nil {
					return err3
				}
				if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hK) {
					return fmt.Errorf("inconsistent account history and changesets, kData %x, csKey %x, data %x, hK %x", kData, csKey, data, hK)
				}
				data = data[length.Addr:]
				if len(data) > 0 { // Skip accounts did not exist
					goOn, err = walker(hK, data)
				}
			} else if cmp == 0 {
				goOn, err = walker(k, v)
			}
		}
		if err != nil {
			return err
		}
		if goOn {
			if cmp <= 0 {
				k, v, err1 = mainCursor.Next()
				if err1 != nil {
					return err1
				}
				for k != nil && len(k) > length.Addr {
					k, v, err1 = mainCursor.Next()
					if err1 != nil {
						return err1
					}
				}
			}
			if cmp >= 0 {
				hK0 := hK
				for hK != nil && (bytes.Equal(hK0, hK) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
					hK, tsEnc, _, hV, err1 = hCursor.Next()
					if err1 != nil {
						return err1
					}
				}
			}
		}
	}
	return err

}
