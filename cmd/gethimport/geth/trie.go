package geth

import (
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	NodeTypeExtension = 1
	NodeTypeBranch    = 2
	NodeTypeLeaf      = 3
	NodeTypeValue     = 4
	NodeTypeHash      = 5
)

type TrieNode struct {
	Type int
	Hash []byte
	Key  []byte

	Children []*TrieNode
	Value    []byte
}

func compactToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return compact
	}
	base := keybytesToHex(compact)
	// delete terminator flag
	if base[0] < 2 {
		base = base[:len(base)-1]
	}
	// apply odd flag
	chop := 2 - base[0]&1
	return base[chop:]
}

func keybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

func decodeShortTrie(hash, elems []byte) (*TrieNode, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	key := compactToHex(kbuf)
	if hasTerm(key) {
		// value node
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid value node: %v", err)
		}
		return &TrieNode{
			Type:  NodeTypeLeaf,
			Hash:  hash,
			Key:   key,
			Value: val,
		}, nil
	}
	r, _, err := decodeRef(rest)
	if err != nil {
		return nil, err
	}
	return &TrieNode{
		Type:     NodeTypeExtension,
		Hash:     hash,
		Key:      key,
		Children: []*TrieNode{r},
	}, nil
}

const hashLen = len(common.Hash{})

func decodeRef(buf []byte) (*TrieNode, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return &TrieNode{
			Type:  NodeTypeHash,
			Value: val,
		}, rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

func decodeFullTrie(hash, elems []byte) (*TrieNode, error) {
	n := &TrieNode{
		Type:     NodeTypeBranch,
		Hash:     hash,
		Children: make([]*TrieNode, 17),
	}
	for i := 0; i < 16; i++ {
		cld, rest, err := decodeRef(elems)
		if err != nil {
			return n, err
		}
		n.Children[i], elems = cld, rest
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = &TrieNode{
			Type:  NodeTypeValue,
			Value: val,
		}
	}
	return n, nil
}

func decodeNode(hash, buf []byte) (*TrieNode, error) {
	if len(buf) == 0 {
		return nil, io.EOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := decodeShortTrie(hash, elems)
		return n, err
	case 17:
		n, err := decodeFullTrie(hash, elems)
		return n, err
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

func decodeNibbles(nibbles []byte, bytes []byte) {
	for bi, ni := 0, 0; ni < len(nibbles); bi, ni = bi+1, ni+2 {
		bytes[bi] = nibbles[ni]<<4 | nibbles[ni+1]
	}
}

// hexToKeybytes turns hex nibbles into key bytes.
// This can only be used for keys of even length.
func HexToKeybytes(hex []byte) []byte {
	if hasTerm(hex) {
		hex = hex[:len(hex)-1]
	}
	if len(hex)&1 != 0 {
		panic("can't convert hex key of odd length")
	}
	key := make([]byte, len(hex)/2)
	decodeNibbles(hex, key)
	return key
}

func TraverseTrie(prefix []byte, n *TrieNode, f func([]byte, *TrieNode) error, reader func([]byte) (*TrieNode, error)) error {
	if n == nil {
		return nil
	}
	currPrefix := append(prefix, n.Key...)
	err := f(currPrefix, n)
	if err != nil {
		return err
	}
	for i, c := range n.Children {
		if c == nil {
			continue
		}
		if n.Type == NodeTypeBranch {
			currPrefix = append(prefix, byte(i))
		}
		if c.Type == NodeTypeHash {
			var child *TrieNode
			child, err = reader(c.Value)
			if err != nil {
				return err
			}
			err = TraverseTrie(currPrefix, child, f, reader)
		} else {
			err = TraverseTrie(currPrefix, c, f, reader)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// prefixLen returns the length of the common prefix of a and b.
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

// hasTerm returns whether a hex key has the terminator flag.
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}
