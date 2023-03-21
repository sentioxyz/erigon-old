package types

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/rlp"
)

type DepositTx struct {
	time time.Time // Time first seen locally (spam avoidance)
	// caches
	hash atomic.Value //nolint:structcheck
	size atomic.Value //nolint:structcheck
	// SourceHash uniquely identifies the source of the deposit
	SourceHash libcommon.Hash
	// From is exposed through the types.Signer, not through TxData
	From libcommon.Address
	// nil means contract creation
	To *libcommon.Address `rlp:"nil"`
	// Mint is minted on L2, locked on L1, nil if no minting.
	Mint *uint256.Int
	// Value is transferred from L2 balance, executed after Mint (if any)
	Value *uint256.Int
	// gas limit
	Gas uint64
	// Field indicating if this transaction is exempt from the L2 gas limit.
	IsSystemTx bool
	// Normal Tx data
	Data []byte
}

var _ Transaction = (*DepositTx)(nil)

func (tx DepositTx) GetChainID() *uint256.Int {
	panic("deposits are not signed and do not have a chain-ID")
}

func (tx DepositTx) GetNonce() uint64 {
	return 0
}

func (tx DepositTx) GetTo() *libcommon.Address {
	return tx.To
}

func (tx DepositTx) GetGas() uint64 {
	return tx.Gas
}

func (tx DepositTx) GetValue() *uint256.Int {
	return tx.Value
}

func (tx DepositTx) GetData() []byte {
	return tx.Data
}

func (tx DepositTx) GetSender() (libcommon.Address, bool) {
	return tx.From, false
}

func (tx *DepositTx) SetSender(addr libcommon.Address) {
	tx.From = addr
}

func (tx DepositTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	panic("deposit tx does not have a signature")
}

func (tx DepositTx) SigningHash(chainID *big.Int) libcommon.Hash {
	panic("deposit tx does not have a signing hash")
}

// NOTE: Need to check this
func (tx DepositTx) EncodingSize() int {
	var buf bytes.Buffer
	if err := tx.MarshalBinary(&buf); err != nil {
		panic(err)
	}
	return len(buf.Bytes())
}

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
func (tx DepositTx) MarshalBinary(w io.Writer) error {
	if _, err := w.Write([]byte{DepositTxType}); err != nil {
		return err
	}
	if err := tx.encodePayload(w); err != nil {
		return err
	}
	return nil
}

func (tx DepositTx) encodePayload(w io.Writer) error {
	return rlp.Encode(w, []interface{}{
		tx.SourceHash,
		tx.From,
		tx.To,
		tx.Mint,
		tx.Value,
		tx.Gas,
		tx.IsSystemTx,
		tx.Data,
	})
}

func (tx DepositTx) EncodeRLP(w io.Writer) error {
	return tx.MarshalBinary(w)
}

func (tx *DepositTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	// SourceHash
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for Source hash: %d", len(b))
	}
	copy(tx.SourceHash[:], b)
	// From
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for From hash: %d", len(b))
	}
	copy(tx.From[:], b)
	// To (optional)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		tx.To = &libcommon.Address{}
		copy((*tx.To)[:], b)
	}
	// Mint
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.Mint = new(uint256.Int).SetBytes(b)
	// Value
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	// Gas
	if tx.Gas, err = s.Uint(); err != nil {
		return err
	}
	if tx.IsSystemTx, err = s.Bool(); err != nil {
		return err
	}
	// Data
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	return s.ListEnd()
}

func (tx *DepositTx) FakeSign(address libcommon.Address) (Transaction, error) {
	cpy := tx.copy()
	cpy.SetSender(address)
	return cpy, nil
}

func (tx *DepositTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	return tx.copy(), nil
}

func (tx DepositTx) Time() time.Time {
	return tx.time
}

func (tx DepositTx) Type() byte { return DepositTxType }

func (tx *DepositTx) Hash() libcommon.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := prefixedRlpHash(DepositTxType, []interface{}{
		tx.SourceHash,
		tx.From,
		tx.To,
		tx.Mint,
		tx.Value,
		tx.Gas,
		tx.IsSystemTx,
		tx.Data,
	})
	tx.hash.Store(&hash)
	return hash
}

func (tx DepositTx) Protected() bool {
	return true
}

func (tx DepositTx) IsContractDeploy() bool {
	return false
}

// All zero in the prototype
func (tx DepositTx) GetPrice() *uint256.Int  { return uint256.NewInt(0) }
func (tx DepositTx) GetTip() *uint256.Int    { return uint256.NewInt(0) }
func (tx DepositTx) GetFeeCap() *uint256.Int { return uint256.NewInt(0) }

// Is this needed at all?
func (tx DepositTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return uint256.NewInt(0)
}

func (tx DepositTx) Cost() *uint256.Int {
	return tx.Value.Clone()
}

func (tx DepositTx) GetAccessList() types2.AccessList {
	return nil
}

// NewDepositTransaction creates a deposit transaction
func NewDepositTransaction(
	sourceHash libcommon.Hash,
	from libcommon.Address,
	to libcommon.Address,
	mint *uint256.Int,
	value *uint256.Int,
	gasLimit uint64,
	isSystemTx bool,
	data []byte) *DepositTx {
	return &DepositTx{
		SourceHash: sourceHash,
		From:       from,
		To:         &to,
		Mint:       mint,
		Value:      value,
		Gas:        gasLimit,
		IsSystemTx: isSystemTx,
		Data:       data,
	}
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx DepositTx) copy() *DepositTx {
	cpy := &DepositTx{
		SourceHash: tx.SourceHash,
		From:       tx.From,
		To:         tx.To,
		Mint:       nil,
		Value:      new(uint256.Int),
		Gas:        tx.Gas,
		IsSystemTx: tx.IsSystemTx,
		Data:       libcommon.Copy(tx.Data),
	}
	if tx.Mint != nil {
		cpy.Mint = new(uint256.Int).Set(tx.Mint)
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	return cpy
}

// AsMessage returns the transaction as a core.Message.
func (tx DepositTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg := Message{
		nonce:         0,
		gasLimit:      tx.Gas,
		gasPrice:      *uint256.NewInt(0),
		tip:           *uint256.NewInt(0),
		feeCap:        *uint256.NewInt(0),
		from:          tx.From,
		to:            tx.To,
		amount:        *tx.Value,
		data:          tx.Data,
		accessList:    nil,
		checkNonce:    true,
		isSystemTx:    tx.IsSystemTx,
		isDepositTx:   true,
		mint:          tx.Mint,
		rollupDataGas: &RollupGasData{},
	}
	return msg, nil
}

func (tx *DepositTx) Sender(signer Signer) (libcommon.Address, error) {
	return tx.From, nil
}
