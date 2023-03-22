package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/ledgerwatch/secp256k1"
	"github.com/spf13/cobra"
	"github.com/torquem-ch/mdbx-go/mdbx"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon/cmd/gethimport/geth"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

var rootCmd = &cobra.Command{
	Use:   "gethimport",
	Short: "Import blockchain data from geth",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		run(cmd, args)
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		defer debug.Exit()
	},
}

func RootCommand() *cobra.Command {
	withDataDir(rootCmd)
	withChain(rootCmd)

	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	return rootCmd
}

func dbCfg(label kv.Label, path string) kv2.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := kv2.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	if label == kv.ChainDB {
		opts = opts.MapSize(8 * datasize.TB)
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts
}

func openDB(opts kv2.MdbxOpts, applyMigrations bool) kv.RwDB {
	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	// to read all options from DB, instead of overriding them
	opts = opts.Flags(func(f uint) uint { return f | mdbx.Accede })

	db := opts.MustOpen()
	if applyMigrations {
		migrator := migrations.NewMigrator(opts.GetLabel())
		has, err := migrator.HasPendingMigrations(db)
		if err != nil {
			panic(err)
		}
		if has {
			log.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			db.Close()
			db = opts.Exclusive().MustOpen()
			if err := migrator.Apply(db, datadirCli); err != nil {
				panic(err)
			}
			db.Close()
			db = opts.MustOpen()
		}
	}
	return db
}

var invalidSender, validSender, numTx, numReceipt uint64
var numImported, latestNumber uint64

func runBatch(config *chain2.Config, db kv.RwDB, gethDB *geth.DB, start, end uint64) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	log.Info("New batch", "start", start, "end", end)
	cryptoContext := secp256k1.DefaultContext
	batchImported := 0
	for i := start; i < end; i++ {
		number := i
		hash, err := gethDB.ReadAncientHeaderHash(number)
		if err != nil {
			return err
		}
		if hash == (libcommon.Hash{}) {
			hash = gethDB.ReadCanonicalHash(number)
		}
		if hash == (libcommon.Hash{}) {
			return fmt.Errorf("cannot find hash for block %d in geth db", number)
		}
		block, err := gethDB.ReadBlock(hash, number)
		if err != nil {
			return err
		}
		if block == nil {
			return errors.New("cannot find block in geth db")
		}
		td := gethDB.ReadTd(hash, number)
		if td == nil {
			return errors.New("cannot find td in geth db")
		}

		if err := rawdb.WriteCanonicalHash(tx, hash, number); err != nil {
			return err
		}
		if err := rawdb.WriteBlock(tx, block); err != nil {
			return err
		}
		if err := rawdb.WriteTd(tx, hash, number, td); err != nil {
			return err
		}
		receipts, err := gethDB.ReadRawReceipts(hash, number)
		if err != nil {
			return err
		}
		if len(receipts) != len(block.Transactions()) {
			return fmt.Errorf("receipts count mismatch: %d != %d", len(receipts), len(block.Transactions()))
		}
		if receipts != nil {
			if err = rawdb.WriteReceipts(tx, number, receipts); err != nil {
				return err
			}
		}

		signer := types.MakeSigner(config, number)
		senders := make([]libcommon.Address, len(block.Transactions()))
		for i, tx := range block.Transactions() {
			from, err := signer.SenderWithContext(cryptoContext, tx)
			if err == nil {
				validSender++
			} else {
				invalidSender++
			}
			senders[i] = from
		}
		if err = rawdb.WriteSenders(tx, block.Hash(), block.NumberU64(), senders); err != nil {
			return err
		}

		numTx += uint64(len(block.Transactions()))
		numReceipt += uint64(len(receipts))
		latestNumber = block.NumberU64()
		numImported++
		batchImported++

		if number%1000 == 0 {
			log.Info("Imported blocks", "numImported", numImported, "lastNumber", number,
				"numTx", numTx, "numReceipt", numReceipt, "invalidSender", invalidSender, "validSender", validSender)
		}
	}
	if batchImported > 0 {
		if err := stages.SaveStageProgress(tx, stages.Headers, latestNumber); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(tx, stages.BlockHashes, latestNumber); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(tx, stages.Senders, latestNumber); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(tx, stages.Senders, latestNumber); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func importAccount(tx kv.RwTx, gethDB *geth.DB, gethAccount *geth.StateAccount, accountHash libcommon.Hash, address libcommon.Address,
	hashPreimage map[libcommon.Hash]libcommon.Hash) error {
	// Import account data.
	hasStorage := gethAccount.Root != trie.EmptyRoot
	account := accounts.NewAccount()
	account.Initialised = true
	account.Nonce = gethAccount.Nonce
	account.Root = gethAccount.Root
	account.CodeHash = libcommon.BytesToHash(gethAccount.CodeHash)
	if gethAccount.Balance != nil {
		account.Balance = *gethAccount.Balance
	}
	hasCode := !account.IsEmptyCodeHash()
	if hasCode || hasStorage {
		account.Incarnation = state.FirstContractIncarnation
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if err := tx.Put(kv.PlainState, address[:], value); err != nil {
		return err
	}

	// Import account code.
	var codeSize uint64
	if hasCode {
		code := gethDB.ReadCode(account.CodeHash)
		codeSize = uint64(len(code))
		if err := tx.Put(kv.Code, account.CodeHash[:], code); err != nil {
			return err
		}
		if err := tx.Put(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], account.Incarnation), account.CodeHash[:]); err != nil {
			return err
		}
	}

	// Import account storage.
	var storageSize uint64
	if hasStorage {
		var err error
		if storageSize, err = importAccountStorage(tx, gethDB, address, &account, hashPreimage); err != nil {
			return err
		}
	}

	log.Info("Imported account", "hash", accountHash, "address", address,
		"hasCode", hasCode, "hasStorage", hasStorage, "codeSize", codeSize, "storageSize", storageSize)
	return nil
}

func importAccountStorage(tx kv.RwTx, gethDB *geth.DB, address libcommon.Address, account *accounts.Account,
	hashPreimage map[libcommon.Hash]libcommon.Hash) (uint64, error) {
	accountRoot, err := gethDB.GetTrieNode(account.Root[:])
	if err != nil {
		return 0, err
	}
	if accountRoot == nil {
		return 0, fmt.Errorf("cannot find storage root %s", account.Root)
	}
	var storageSize uint64
	if err = geth.TraverseTrie(nil, accountRoot, func(prefix []byte, node *geth.TrieNode) error {
		if node.Type != geth.NodeTypeLeaf && node.Type != geth.NodeTypeValue {
			return nil
		}
		decodedStorageKey := geth.HexToKeybytes(prefix)
		if len(decodedStorageKey) != length.Hash {
			return fmt.Errorf("invalid storage key length: %d", len(decodedStorageKey))
		}
		originalKey, ok := hashPreimage[libcommon.BytesToHash(decodedStorageKey)]
		if !ok {
			return fmt.Errorf("cannot find preimage for storage key %x", decodedStorageKey)
		}
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), account.Incarnation, originalKey[:])
		_, content, _, err := rlp.Split(node.Value)
		if err != nil {
			return err
		}
		value := new(uint256.Int).SetBytes(content).Bytes()
		storageSize++
		if storageSize%10000 == 0 {
			log.Info("Imported storage", "address", address, "storageSize", storageSize)
		}
		return tx.Put(kv.PlainState, compositeKey, value)
	}, func(h []byte) (*geth.TrieNode, error) {
		return gethDB.GetTrieNode(h)
	}); err != nil {
		return storageSize, err
	}
	return storageSize, nil
}

func run(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	chaindata := filepath.Join(datadirCli, "chaindata")
	db := openDB(dbCfg(kv.ChainDB, chaindata), true)
	defer db.Close()

	gethDB := geth.NewDB(gethDatadir)
	defer gethDB.Close()

	// Verify genesis.
	var config *chain2.Config
	db.View(ctx, func(tx kv.Tx) error {
		genesis, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			panic(err)
		}
		if genesis == nil {
			panic("genesis is not found")
		}
		gethGenesis, _ := gethDB.ReadBlock(genesis.Hash(), genesis.NumberU64())
		if gethGenesis == nil {
			panic("genesis is not found in geth db")
		}
		if !bytes.Equal(gethGenesis.Hash().Bytes(), genesis.Hash().Bytes()) {
			panic("genesis hashes are not equal")
		}
		if config, err = rawdb.ReadChainConfig(tx, genesis.Hash()); err != nil {
			panic("no chain config in genesis")
		}
		log.Info("Verified genesis", "hash", genesis.Hash())
		return nil
	})

	headHash := gethDB.ReadHeadBlockHash()
	headNumber := *(gethDB.ReadHeaderNumber(headHash))
	headHeader, err := gethDB.ReadHeader(headHash, headNumber)
	if err != nil {
		panic(err)
	}
	log.Info("Got head", "hash", headHash, "number", headNumber, "stateRoot", headHeader.Root)

	// Import blocks.
	var start uint64
	if err = db.View(ctx, func(tx kv.Tx) error {
		start, err = stages.GetStageProgress(tx, stages.Headers)
		return err
	}); err != nil {
		panic(err)
	}
	if start > 0 {
		start++
	}
	batchSize := uint64(100000)
	for start <= headNumber {
		end := start + uint64(batchSize)
		if end > headNumber+1 {
			end = headNumber + 1
		}
		if err := runBatch(config, db, gethDB, start, end); err != nil {
			panic(err)
		}
		start = end
	}

	// Recover account addresses.
	hashPreimage := make(map[libcommon.Hash]libcommon.Hash)
	accountHashToAddress := make(map[libcommon.Hash]libcommon.Address)
	populateAddr := func(addr libcommon.Address) {
		hash, err := common.HashData(addr[:])
		if err != nil {
			panic(err)
		}
		accountHashToAddress[hash] = addr
	}
	if err = gethDB.ForEachPreimage(func(hash libcommon.Hash, value []byte) error {
		if len(value) == length.Addr {
			var addr libcommon.Address
			copy(addr[:], value)
			populateAddr(addr)
		} else if len(value) == length.Hash {
			hashPreimage[hash] = libcommon.BytesToHash(value)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	log.Info("Recovered preimage", "numAccounts", len(accountHashToAddress), "numHashes", len(hashPreimage))

	// Import states.
	var stateLatestNumber uint64
	if err = db.View(ctx, func(tx kv.Tx) error {
		stateLatestNumber, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}); err != nil {
		panic(err)
	}
	if stateLatestNumber >= headNumber {
		log.Info("State is already imported")
	} else {
		if err = db.Update(ctx, func(tx kv.RwTx) error {
			return tx.ClearBucket(kv.PlainState)
		}); err != nil {
			panic(err)
		}
		var numAccounts uint64
		root, err := gethDB.GetTrieNode(headHeader.Root[:])
		if err != nil {
			panic(err)
		}
		if err = geth.TraverseTrie(nil, root, func(prefix []byte, node *geth.TrieNode) error {
			if node.Type != geth.NodeTypeLeaf {
				return nil
			}
			tx, err := db.BeginRw(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			accountHashBytes := geth.HexToKeybytes(prefix)
			if len(accountHashBytes) != 32 {
				return fmt.Errorf("invalid account hash length %d", len(accountHashBytes))
			}
			accountHash := libcommon.BytesToHash(accountHashBytes)
			gethAccount := &geth.StateAccount{}
			if err := rlp.DecodeBytes(node.Value, gethAccount); err != nil {
				return err
			}
			address, ok := accountHashToAddress[accountHash]
			if !ok {
				return fmt.Errorf("cannot find account address for hash %s", accountHash)
			}
			if err = importAccount(tx, gethDB, gethAccount, accountHash, address, hashPreimage); err != nil {
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}
			numAccounts++
			return nil
		}, func(h []byte) (*geth.TrieNode, error) {
			return gethDB.GetTrieNode(h)
		}); err != nil {
			panic(err)
		}
		if err = db.Update(ctx, func(tx kv.RwTx) error {
			return stages.SaveStageProgress(tx, stages.Execution, headNumber)
		}); err != nil {
			panic(err)
		}
		log.Info("Imported states", "numAccounts", numAccounts, "headNumber", headNumber)
	}

	// Run HashState stage.
	dirs, historyV3 := datadir.New(datadirCli), kvcfg.HistoryV3.FromDB(db)
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if err = tx.ClearBucket(kv.HashedAccounts); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.HashedStorage); err != nil {
			return err
		}
		cfg := stagedsync.StageHashStateCfg(db, dirs, historyV3, nil)
		return stagedsync.PromoteHashedStateCleanly("HashState", tx, cfg, ctx)
	}); err != nil {
		panic(err)
	}
	log.Info("Finished HashState")

	// Run InterHashState stage.
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		cfg := stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, nil, nil, historyV3, nil)
		var root libcommon.Hash
		if root, err = stagedsync.RegenerateIntermediateHashes("InterHashState", tx, cfg, headHeader.Root, ctx); err != nil {
			return err
		}
		if !bytes.Equal(root[:], headHeader.Root[:]) {
			return fmt.Errorf("regenerated root %s is not equal to head root %s", root, headHeader.Root)
		}
		log.Info("Regenerated intermediate hashes")
		return nil
	}); err != nil {
		panic(err)
	}
}
