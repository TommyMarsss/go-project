package kvstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testOpts disables background maintenance so tests stay deterministic.
func testOpts() *Options {
	return &Options{SyncWrites: true}
}

func openTest(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(dir, testOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	val, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(val) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, val, want)
	}
}

func mustNotFound(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) = %v, want ErrNotFound", key, err)
	}
}

func TestBasicCRUDAndReopen(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)

	if err := db.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1")
	mustNotFound(t, db, "missing")

	// Update and delete.
	if err := db.Put("a", []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("b"); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "3")
	mustNotFound(t, db, "b")

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: state must be recovered from the WAL.
	db = openTest(t, dir)
	defer db.Close()
	mustGet(t, db, "a", "3")
	mustNotFound(t, db, "b")
}

func TestSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	if err := db.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// A read transaction pins a snapshot.
	rtx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()

	// Concurrent writer updates and deletes keys.
	if err := db.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("new", []byte("x")); err != nil {
		t.Fatal(err)
	}

	// The snapshot must be stable.
	val, err := rtx.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Fatalf("snapshot read = %q, want v1", val)
	}
	if _, err := rtx.Get("new"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot must not see later writes, got %v", err)
	}

	// A new transaction sees the latest state.
	mustGet(t, db, "k", "v2")
}

func TestWriteWriteConflict(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	if err := db.Put("k", []byte("base")); err != nil {
		t.Fatal(err)
	}

	tx1, _ := db.Begin(false)
	tx2, _ := db.Begin(false)

	if err := tx1.Put("k", []byte("from-tx1")); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Put("k", []byte("from-tx2")); err != nil {
		t.Fatal(err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("first committer must win: %v", err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("second committer must get ErrConflict, got %v", err)
	}

	// The loser's dirty data must not be visible.
	mustGet(t, db, "k", "from-tx1")

	// Disjoint write sets do not conflict.
	tx3, _ := db.Begin(false)
	tx4, _ := db.Begin(false)
	tx3.Put("x", []byte("1"))
	tx4.Put("y", []byte("2"))
	if err := tx3.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx4.Commit(); err != nil {
		t.Fatalf("disjoint writes must not conflict: %v", err)
	}
}

func TestReadYourOwnWritesAndRollback(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	tx, _ := db.Begin(false)
	tx.Put("k", []byte("staged"))
	val, err := tx.Get("k")
	if err != nil || string(val) != "staged" {
		t.Fatalf("read-own-write = %q, %v", val, err)
	}
	// Uncommitted data is invisible to others.
	mustNotFound(t, db, "k")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, db, "k")
	if _, err := tx.Get("k"); !errors.Is(err, ErrTxDone) {
		t.Fatalf("use after rollback = %v, want ErrTxDone", err)
	}
}

func TestIteratorSnapshotStability(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("key%02d", i), []byte(fmt.Sprintf("v%d", i)))
	}

	tx, _ := db.Begin(true)
	defer tx.Rollback()
	it, err := tx.NewIterator("", "")
	if err != nil {
		t.Fatal(err)
	}

	// Mutate heavily after iterator creation: overwrite, delete, insert,
	// then run version GC and a checkpoint.
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("key%02d", i), []byte("overwritten"))
	}
	db.Delete("key00")
	db.Put("zz-new", []byte("new"))
	db.Compact()
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// The iterator must still yield the original snapshot, in order.
	n := 0
	for ; it.Valid(); it.Next() {
		wantKey := fmt.Sprintf("key%02d", n)
		wantVal := fmt.Sprintf("v%d", n)
		if it.Key() != wantKey || string(it.Value()) != wantVal {
			t.Fatalf("iter[%d] = %q=%q, want %q=%q", n, it.Key(), it.Value(), wantKey, wantVal)
		}
		n++
	}
	if n != 10 {
		t.Fatalf("iterator yielded %d entries, want 10", n)
	}
}

func TestIteratorRangeAndSeek(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		db.Put(k, []byte("val-"+k))
	}
	tx, _ := db.Begin(true)
	defer tx.Rollback()

	it, err := tx.NewIterator("b", "e") // [b, e)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, it.Key())
	}
	if fmt.Sprint(got) != "[b c d]" {
		t.Fatalf("range scan = %v, want [b c d]", got)
	}

	it.Seek("d")
	if !it.Valid() || it.Key() != "d" {
		t.Fatalf("Seek(d) landed on %v", it.Key())
	}
	it.Seek("zzz")
	if it.Valid() {
		t.Fatal("Seek past end must be invalid")
	}
}

func TestCrashRecoveryReplay(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)

	db.Put("committed-1", []byte("a"))
	db.Put("committed-2", []byte("b"))

	// An uncommitted transaction must leave no trace after recovery.
	tx, _ := db.Begin(false)
	tx.Put("uncommitted", []byte("x"))

	// Simulate a crash: no Close, no checkpoint — just reopen the dir.
	// (The abandoned handle keeps its file descriptors; harmless here.)
	db2 := openTest(t, dir)
	defer db2.Close()

	mustGet(t, db2, "committed-1", "a")
	mustGet(t, db2, "committed-2", "b")
	mustNotFound(t, db2, "uncommitted")

	// New writes after recovery must continue with fresh seq numbers.
	if err := db2.Put("after-crash", []byte("c")); err != nil {
		t.Fatal(err)
	}
	db2.Close()

	db3 := openTest(t, dir)
	defer db3.Close()
	mustGet(t, db3, "committed-1", "a")
	mustGet(t, db3, "after-crash", "c")
}

func TestTornWALTail(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	db.Put("k1", []byte("v1"))
	db.Put("k2", []byte("v2"))
	db.Close()

	// Simulate a torn write: garbage appended to the WAL.
	f, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db = openTest(t, dir)
	defer db.Close()
	mustGet(t, db, "k1", "v1")
	mustGet(t, db, "k2", "v2")

	// The store must still accept writes after truncating the torn tail.
	if err := db.Put("k3", []byte("v3")); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db = openTest(t, dir)
	defer db.Close()
	mustGet(t, db, "k3", "v3")
}

func TestCheckpointAndWALTruncation(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)

	for i := 0; i < 50; i++ {
		db.Put(fmt.Sprintf("k%02d", i), []byte(fmt.Sprintf("v%d", i)))
	}
	db.Delete("k00")

	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// WAL must have been truncated.
	fi, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("WAL size after checkpoint = %d, want 0", fi.Size())
	}

	// Post-checkpoint writes go to the fresh WAL.
	db.Put("post", []byte("ckpt"))
	db.Close()

	// Recovery: checkpoint + incremental WAL replay.
	db = openTest(t, dir)
	defer db.Close()
	for i := 1; i < 50; i++ {
		mustGet(t, db, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}
	mustNotFound(t, db, "k00")
	mustGet(t, db, "post", "ckpt")
}

func TestInterruptedCheckpointIsSafe(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	db.Put("k", []byte("v"))
	db.Close()

	// Simulate a crash mid-checkpoint: a half-written tmp file exists.
	if err := os.WriteFile(filepath.Join(dir, ckptTmpName), []byte("partial garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db = openTest(t, dir)
	defer db.Close()
	mustGet(t, db, "k", "v")
	// The stale tmp file must have been cleaned up.
	if _, err := os.Stat(filepath.Join(dir, ckptTmpName)); !os.IsNotExist(err) {
		t.Fatalf("stale tmp checkpoint should be removed, stat err = %v", err)
	}
}

func TestCompactionKeepsSnapshotVersions(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	db.Put("k", []byte("v1"))

	// Pin an old snapshot.
	rtx, _ := db.Begin(true)

	// Overwrite many times and reclaim.
	for i := 2; i <= 20; i++ {
		db.Put("k", []byte(fmt.Sprintf("v%d", i)))
	}
	db.Compact()

	// The pinned snapshot must still read its version.
	val, err := rtx.Get("k")
	if err != nil || string(val) != "v1" {
		t.Fatalf("snapshot after compaction = %q, %v; want v1", val, err)
	}
	rtx.Rollback()

	// With no active snapshots, GC may drop everything obsolete.
	db.Compact()
	mustGet(t, db, "k", "v20")

	// Tombstones are reclaimed once no snapshot can see them.
	db.Delete("k")
	db.Compact()
	mustNotFound(t, db, "k")
	if n := db.index.size; n != 0 {
		t.Fatalf("index size after GC = %d, want 0 (tombstone reclaimed)", n)
	}
}

func TestAutoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		SyncWrites:      true,
		GCInterval:      10 * time.Millisecond,
		CheckpointBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	big := make([]byte, 256)
	for i := 0; i < 100; i++ {
		if err := db.Put(fmt.Sprintf("key%03d", i), big); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for the background loop to checkpoint.
	deadline := time.Now().Add(5 * time.Second)
	for {
		fi, err := os.Stat(filepath.Join(dir, ckptName))
		if err == nil && fi.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("auto-checkpoint did not happen")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// All data must survive a reopen.
	db.Close()
	db = openTest(t, dir)
	defer db.Close()
	for i := 0; i < 100; i++ {
		val, err := db.Get(fmt.Sprintf("key%03d", i))
		if err != nil || len(val) != 256 {
			t.Fatalf("key%03d: len=%d err=%v", i, len(val), err)
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	const workers = 8
	const rounds = 100

	var wg sync.WaitGroup
	// Writers: disjoint keys, so no conflicts expected.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				key := fmt.Sprintf("w%d-key%d", w, r)
				if err := db.Put(key, []byte(fmt.Sprintf("val%d", r))); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}
	// Readers: snapshot reads and scans must never fail or see garbage.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				tx, err := db.Begin(true)
				if err != nil {
					t.Errorf("Begin: %v", err)
					return
				}
				it, err := tx.NewIterator("", "")
				if err != nil {
					t.Errorf("NewIterator: %v", err)
					tx.Rollback()
					return
				}
				prev := ""
				for ; it.Valid(); it.Next() {
					if it.Key() <= prev {
						t.Errorf("iterator out of order: %q after %q", it.Key(), prev)
						break
					}
					prev = it.Key()
				}
				tx.Rollback()
			}
		}()
	}
	// Conflicting writers: exactly one of each pair may commit.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			tx1, _ := db.Begin(false)
			tx2, _ := db.Begin(false)
			tx1.Put("hot", []byte("tx1"))
			tx2.Put("hot", []byte("tx2"))
			err1 := tx1.Commit()
			err2 := tx2.Commit()
			if err1 != nil && err2 != nil {
				t.Errorf("both conflicting txns failed: %v / %v", err1, err2)
			}
		}
	}()
	wg.Wait()

	// Spot-check final state.
	mustGet(t, db, "w3-key42", "val42")
	if _, err := db.Get("hot"); err != nil {
		t.Fatalf("hot key must exist: %v", err)
	}
}

func TestCheckpointDuringActiveSnapshot(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer db.Close()

	db.Put("k", []byte("old"))
	rtx, _ := db.Begin(true) // pins snapshot at "old"
	defer rtx.Rollback()

	db.Put("k", []byte("new"))
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	db.Compact()

	// Old snapshot still reads its version even after checkpoint + GC.
	val, err := rtx.Get("k")
	if err != nil || string(val) != "old" {
		t.Fatalf("snapshot read = %q, %v; want old", val, err)
	}
	mustGet(t, db, "k", "new")
}
