package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"cosmossdk.io/log"
	storeiavl "cosmossdk.io/store/iavl"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/snapshots"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	storetypes "cosmossdk.io/store/types"
	db "github.com/cosmos/cosmos-db"
	"github.com/cosmos/gogoproto/proto"
	iavltree "github.com/cosmos/iavl"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var errBenchmarkComplete = errors.New("bounded benchmark complete")

type progressWriter struct {
	stream     *snapshots.StreamWriter
	store      string
	nodes      uint64
	evmNodes   uint64
	limit      uint64
	seconds    time.Duration
	started    time.Time
	evmStarted time.Time
	sample     hash.Hash
}

func report(value any) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		panic(err)
	}
}

func (w *progressWriter) WriteMsg(message proto.Message) error {
	item, ok := message.(*snapshottypes.SnapshotItem)
	if !ok {
		return fmt.Errorf("unexpected snapshot item type %T", message)
	}
	switch item := item.Item.(type) {
	case *snapshottypes.SnapshotItem_Store:
		w.store = item.Store.Name
		report(map[string]any{"event": "store_start", "store": w.store, "total_nodes": w.nodes})
		if w.store == "evm" {
			w.evmStarted = time.Now()
		}
	case *snapshottypes.SnapshotItem_IAVL:
		w.nodes++
		if w.store == "evm" {
			w.evmNodes++
			if w.evmNodes <= 10000 {
				encoded, err := proto.Marshal(message)
				if err != nil {
					return err
				}
				w.sample.Write(encoded)
			}
		}
		if w.nodes%100000 == 0 {
			w.progress("progress")
		}
	}
	if err := w.stream.WriteMsg(message); err != nil {
		return err
	}
	if w.store == "evm" && w.limit > 0 && w.evmNodes >= w.limit {
		return errBenchmarkComplete
	}
	if w.seconds > 0 && w.nodes%10000 == 0 && time.Since(w.started) >= w.seconds {
		return errBenchmarkComplete
	}
	return nil
}

func (w *progressWriter) progress(event string) {
	r := map[string]any{
		"event": event, "at_utc": time.Now().UTC(), "store": w.store,
		"nodes": w.nodes, "evm_nodes": w.evmNodes,
		"elapsed_seconds": time.Since(w.started).Seconds(),
		"EVM_first10000_protobuf_SHA256": hex.EncodeToString(w.sample.Sum(nil)),
	}
	if !w.evmStarted.IsZero() {
		r["evm_nodes_per_second"] = float64(w.evmNodes) / time.Since(w.evmStarted).Seconds()
	}
	report(r)
}

func openRoot(source string, cacheMiB, openFiles int) (*rootmulti.Store, db.DB, error) {
	if filepath.Base(source) != "application.db" {
		return nil, nil, errors.New("source must name application.db")
	}
	application, err := db.NewGoLevelDBWithOpts("application", filepath.Dir(source), &opt.Options{
		ReadOnly: true, DisableSeeksCompaction: true,
		BlockCacheCapacity: cacheMiB * 1024 * 1024,
		OpenFilesCacheCapacity: openFiles,
		Filter: filter.NewBloomFilter(10),
	})
	if err != nil {
		return nil, nil, err
	}
	root := rootmulti.NewStore(application, log.NewNopLogger(), metrics.NewNoOpMetrics())
	root.SetIAVLCacheSize(1024)
	root.SetIAVLDisableFastNode(true)
	root.SetIAVLSyncPruning(true)
	height := rootmulti.GetLatestVersion(application)
	if height <= 0 {
		application.Close()
		return nil, nil, errors.New("source has no committed application height")
	}
	info, err := root.GetCommitInfo(height)
	if err != nil {
		application.Close()
		return nil, nil, err
	}
	for _, store := range info.StoreInfos {
		root.MountStoreWithDB(storetypes.NewKVStoreKey(store.Name), storetypes.StoreTypeIAVL, nil)
	}
	if err := root.LoadVersion(height); err != nil {
		application.Close()
		return nil, nil, err
	}
	return root, application, nil
}

func benchmarkEVM(root *rootmulti.Store, writer *progressWriter, height int64) error {
	store, ok := root.GetStoreByName("evm").(*storeiavl.Store)
	if !ok {
		return errors.New("EVM store is unavailable")
	}
	exporter, err := store.Export(height)
	if err != nil {
		return err
	}
	defer exporter.Close()
	if err := writer.WriteMsg(&snapshottypes.SnapshotItem{
		Item: &snapshottypes.SnapshotItem_Store{Store: &snapshottypes.SnapshotStoreItem{Name: "evm"}},
	}); err != nil {
		return err
	}
	for {
		node, err := exporter.Next()
		if err == iavltree.ErrorExportDone {
			return nil
		}
		if err != nil {
			return err
		}
		if err := writer.WriteMsg(&snapshottypes.SnapshotItem{
			Item: &snapshottypes.SnapshotItem_IAVL{IAVL: &snapshottypes.SnapshotIAVLItem{
				Key: node.Key, Value: node.Value, Height: int32(node.Height), Version: node.Version,
			}},
		}); err != nil {
			return err
		}
	}
}

func export(root *rootmulti.Store, directory string, limit uint64, seconds time.Duration) error {
	height := root.LastCommitID().Version
	chunks := make(chan io.ReadCloser)
	stream := snapshots.NewStreamWriter(chunks)
	writer := &progressWriter{
		stream: stream, limit: limit, seconds: seconds,
		started: time.Now(), sample: sha256.New(),
	}
	done := make(chan error, 1)
	go func() {
		var err error
		if limit > 0 || seconds > 0 {
			err = benchmarkEVM(root, writer, height)
		} else {
			err = root.Snapshot(uint64(height), writer)
		}
		if err != nil {
			stream.CloseWithError(err)
		} else {
			err = stream.Close()
		}
		done <- err
	}()
	if limit > 0 || seconds > 0 {
		for chunk := range chunks {
			_, err := io.Copy(io.Discard, chunk)
			chunk.Close()
			if err != nil && !errors.Is(err, errBenchmarkComplete) {
				return err
			}
		}
		err := <-done
		writer.progress("benchmark_complete")
		if errors.Is(err, errBenchmarkComplete) {
			return nil
		}
		return err
	}
	if directory == "" {
		return errors.New("full export requires an output directory")
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	metadata, err := db.NewGoLevelDB("metadata", directory, nil)
	if err != nil {
		return err
	}
	defer metadata.Close()
	store, err := snapshots.NewStore(metadata, directory)
	if err != nil {
		return err
	}
	snapshot, err := store.Save(uint64(height), snapshottypes.CurrentFormat, chunks)
	if err != nil {
		return err
	}
	if err := <-done; err != nil {
		return err
	}
	writer.progress("export_complete")
	encoded, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "snapshot.pb"), encoded, 0600); err != nil {
		return err
	}
	report(map[string]any{"event": "snapshot_created", "snapshot": snapshot})
	return nil
}

func run() error {
	source := flag.String("source", "", "Path to the frozen application.db copy")
	output := flag.String("output", "", "New directory for SDK-compatible chunks and metadata")
	cacheMiB := flag.Int("cache-mib", 1024, "LevelDB block cache capacity in MiB")
	openFiles := flag.Int("open-files", 16384, "LevelDB cached file handles")
	limit := flag.Uint64("max-evm-nodes", 0, "Stop a benchmark after this many EVM nodes")
	seconds := flag.Int("benchmark-seconds", 0, "Bound a benchmark duration")
	inspect := flag.Bool("inspect", false, "Report the committed source state and exit")
	profile := flag.String("cpu-profile", "", "Write a CPU profile for this run")
	workers := flag.Int("export-workers", 1, "Concurrent read-only subtree readers with ordered output")
	flag.Parse()
	if err := iavltree.SetExportWorkers(*workers); err != nil {
		return err
	}
	if *source == "" || *cacheMiB < 1 || *openFiles < 1 || *seconds < 0 {
		return errors.New("source, cache and open-files settings must be valid")
	}
	if *profile != "" {
		file, err := os.Create(*profile)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := pprof.StartCPUProfile(file); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	root, application, err := openRoot(*source, *cacheMiB, *openFiles)
	if err != nil {
		return err
	}
	defer application.Close()
	commit := root.LastCommitID()
	report(map[string]any{
		"event": "source_opened", "height": commit.Version,
		"app_hash": fmt.Sprintf("%X", commit.Hash), "format": snapshottypes.CurrentFormat,
		"cache_mib": *cacheMiB, "open_files": *openFiles, "read_only": true,
		"export_workers": *workers,
	})
	if *inspect {
		return nil
	}
	return export(root, *output, *limit, time.Duration(*seconds)*time.Second)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
