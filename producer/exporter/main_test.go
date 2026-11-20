package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/snapshots"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	storetypes "cosmossdk.io/store/types"
	db "github.com/cosmos/cosmos-db"
	iavltree "github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
)

func TestCLIRejectsMissingSource(t *testing.T) {
	const marker = "BEEHIVE_EXPORTER_TEST_CLI"
	if os.Getenv(marker) == "1" {
		os.Args = []string{"native-export"}
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		main()
		return
	}

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestCLIRejectsMissingSource$")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), marker+"=1")
	stdout, err := cmd.Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("CLI without a source returned %v, stdout: %s", err, stdout)
	}
	if len(stdout) != 0 || !bytes.Contains(exit.Stderr, []byte("source")) {
		t.Fatalf("CLI did not report a missing source on stderr: stdout=%q stderr=%q", stdout, exit.Stderr)
	}
}

type readFaultDB struct {
	iavldb.DB
	readErr error
}

func (db *readFaultDB) Get(key []byte) ([]byte, error) {
	if db.readErr != nil {
		return nil, db.readErr
	}
	return db.DB.Get(key)
}

func TestParallelExportReturnsWorkerReadError(t *testing.T) {
	if err := iavltree.SetExportWorkers(2); err != nil {
		t.Fatal(err)
	}
	defer iavltree.SetExportWorkers(1)

	database := &readFaultDB{DB: iavldb.NewMemDB()}
	tree := iavltree.NewMutableTree(database.DB, 0, true, iavltree.NewNopLogger())
	defer tree.Close()
	for _, key := range []string{"blue", "green"} {
		if _, err := tree.Set([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	_, version, err := tree.SaveVersion()
	if err != nil {
		t.Fatal(err)
	}
	// Reopen to load only the root. The worker has to fetch its children.
	reader := iavltree.NewMutableTree(database, 0, true, iavltree.NewNopLogger())
	defer reader.Close()
	frozen, err := reader.GetImmutable(version)
	if err != nil {
		t.Fatal(err)
	}
	database.readErr = errors.New("fixture could not read the child node")
	exporter, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Close()
	// IAVL includes the underlying read error in its own diagnostic.
	if node, err := exporter.Next(); node != nil || err == nil || !strings.Contains(err.Error(), database.readErr.Error()) {
		t.Fatalf("expected the worker read failure, got node=%v err=%v", node, err)
	}
}

func TestExportWorkerBounds(t *testing.T) {
	defer iavltree.SetExportWorkers(1)
	for _, count := range []int{0, 1, 64, 65} {
		wantValid := count == 1 || count == 64
		if err := iavltree.SetExportWorkers(count); (err == nil) != wantValid {
			t.Errorf("SetExportWorkers(%d) returned %v, want valid=%v", count, err, wantValid)
		}
	}
}

func TestBenchmarkStopsAtEVMLimit(t *testing.T) {
	chunks := make(chan io.ReadCloser)
	stream := snapshots.NewStreamWriter(chunks)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for chunk := range chunks {
			if _, err := io.Copy(io.Discard, chunk); err != nil {
				t.Errorf("reading benchmark output: %v", err)
			}
			chunk.Close()
		}
	}()
	t.Cleanup(func() {
		if err := stream.Close(); err != nil {
			t.Errorf("closing benchmark output: %v", err)
		}
		<-drained
	})
	writer := &progressWriter{stream: stream, store: "evm", limit: 3, sample: sha256.New()}
	item := &snapshottypes.SnapshotItem{
		Item: &snapshottypes.SnapshotItem_IAVL{IAVL: &snapshottypes.SnapshotIAVLItem{
			Key: []byte("key"), Value: []byte("value"), Version: 1,
		}},
	}
	for i := 1; i < 3; i++ {
		if err := writer.WriteMsg(item); err != nil {
			t.Fatalf("benchmark stopped early at node %d: %v", i, err)
		}
	}
	if err := writer.WriteMsg(item); !errors.Is(err, errBenchmarkComplete) {
		t.Fatalf("benchmark reached node 3, got %v, want completion", err)
	}
}

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
	t.Run("source rejects writes", func(t *testing.T) {
		key := []byte("exporter-readonly-probe")
		if found, err := source.Has(key); err != nil || found {
			t.Fatalf("expected a readable database and an unused key: found=%v err=%v", found, err)
		}
		if err := source.Set(key, []byte("test")); err == nil {
			t.Fatal("exporter's source handle accepted a write")
		}
		if found, err := source.Has(key); err != nil || found {
			t.Fatalf("after rejected write: found=%v err=%v", found, err)
		}
	})
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
