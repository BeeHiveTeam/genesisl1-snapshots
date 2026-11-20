package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/snapshots"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	storetypes "cosmossdk.io/store/types"
	db "github.com/cosmos/cosmos-db"
	iavltree "github.com/cosmos/iavl"
)

func TestReadOnlyExportMatchesSDKAndRestores(t *testing.T) {
	if err := iavltree.SetExportWorkers(1); err != nil {
		t.Fatal(err)
	}
	defer iavltree.SetExportWorkers(1)
	directory := t.TempDir()
	application, err := db.NewGoLevelDB("application", directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := rootmulti.NewStore(application, log.NewNopLogger(), metrics.NewNoOpMetrics())
	root.SetIAVLDisableFastNode(true)
	root.SetIAVLSyncPruning(true)
	keys := []*storetypes.KVStoreKey{storetypes.NewKVStoreKey("acc"), storetypes.NewKVStoreKey("evm")}
	for _, key := range keys {
		root.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
	}
	if err := root.LoadLatestVersion(); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		store := root.GetKVStore(key)
		for i := 0; i < 10000; i++ {
			store.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("value-%06d", i)))
		}
	}
	commit := root.Commit()
	standardDirectory := filepath.Join(directory, "standard")
	standardStore, err := snapshots.NewStore(db.NewMemDB(), standardDirectory)
	if err != nil {
		t.Fatal(err)
	}
	manager := snapshots.NewManager(standardStore, snapshottypes.SnapshotOptions{}, root, nil, log.NewNopLogger())
	standard, err := manager.Create(uint64(commit.Version))
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, source, err := openRoot(filepath.Join(directory, "application.db"), 8, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if !bytes.Equal(readonly.LastCommitID().Hash, commit.Hash) {
		t.Fatal("read-only store loaded a different application hash")
	}
	optimizedDirectory := filepath.Join(directory, "optimized")
	if err := iavltree.SetExportWorkers(8); err != nil {
		t.Fatal(err)
	}
	if err := export(readonly, optimizedDirectory, 0, 0); err != nil {
		t.Fatal(err)
	}
	metadata, err := db.NewGoLevelDB("metadata", optimizedDirectory, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	optimizedStore, err := snapshots.NewStore(metadata, optimizedDirectory)
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := optimizedStore.Get(standard.Height, standard.Format)
	if err != nil || optimized == nil {
		t.Fatalf("optimized metadata unavailable: %v", err)
	}
	if !bytes.Equal(standard.Hash, optimized.Hash) || standard.Chunks != optimized.Chunks {
		t.Fatal("optimized output differs from the canonical SDK snapshot")
	}
	restored := rootmulti.NewStore(db.NewMemDB(), log.NewNopLogger(), metrics.NewNoOpMetrics())
	restored.SetIAVLDisableFastNode(true)
	restored.SetIAVLSyncPruning(true)
	for _, key := range keys {
		restored.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
	}
	if err := restored.LoadLatestVersion(); err != nil {
		t.Fatal(err)
	}
	restoreDirectory := filepath.Join(directory, "receiver")
	restoreStore, err := snapshots.NewStore(db.NewMemDB(), restoreDirectory)
	if err != nil {
		t.Fatal(err)
	}
	receiver := snapshots.NewManager(restoreStore, snapshottypes.SnapshotOptions{}, restored, nil, log.NewNopLogger())
	if err := receiver.Restore(*optimized); err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < optimized.Chunks; i++ {
		data, err := os.ReadFile(optimizedStore.PathChunk(optimized.Height, optimized.Format, i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := receiver.RestoreChunk(data); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(restored.LastCommitID().Hash, commit.Hash) {
		t.Fatal("restored application hash differs from original")
	}
	for _, key := range keys {
		for i := 0; i < 10000; i++ {
			actual := restored.GetKVStore(key).Get([]byte(fmt.Sprintf("key-%06d", i)))
			if !bytes.Equal(actual, []byte(fmt.Sprintf("value-%06d", i))) {
				t.Fatal("restored key/value differs")
			}
		}
	}
}
