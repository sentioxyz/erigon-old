package bitmapdb2

import (
	"encoding/binary"
)

// Logical structure of the database:
// AccountsHistory:
//   key: plain unhashed address
//   value: bitmap
// StorageHistory
//   key: plain unhashed address + plain unhashed storage key
//   value: bitmap
// CallFromIndex
//   key: plain unhashed address
//   value: bitmap
// CallToIndex
//   key: plain unhashed address
//   value: bitmap
// LogTopicIndex
//   key: plain unhashed topic
//   value: bitmap
// LogAddressIndex
//   key: plain unhashed address
//   value: bitmap

var (
	BucketKeyPrefix = []byte("B")
	MaxValue        = ^uint64(0)
)

func bitmapRowKey(bucket string, key []byte, sliceMax uint64) []byte {
	rowKey := make([]byte, len(BucketKeyPrefix)+len(bucket)+len(key)+8)
	copy(rowKey, BucketKeyPrefix)
	copy(rowKey[len(BucketKeyPrefix):], bucket)
	copy(rowKey[len(BucketKeyPrefix)+len(bucket):], key)
	binary.BigEndian.PutUint64(rowKey[len(BucketKeyPrefix)+len(bucket)+len(key):], sliceMax)
	return rowKey
}

func bitmapRowPrefix(bucket string, key []byte) []byte {
	prefix := make([]byte, len(BucketKeyPrefix)+len(bucket)+len(key))
	copy(prefix, BucketKeyPrefix)
	copy(prefix[len(BucketKeyPrefix):], bucket)
	copy(prefix[len(BucketKeyPrefix)+len(bucket):], key)
	return prefix
}

func getSliceMaxFromRowKey(rowKey []byte) uint64 {
	return binary.BigEndian.Uint64(rowKey[len(rowKey)-8:])
}
