package kvstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T, opts ...Option) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db, dir
}

func mustPut(t *testing.T, db *DB, key, val string) {
	t.Helper()
	if err := db.Update(func(tx *Txn) error { return tx.Put([]byte(key), []byte(val)) }); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

func mustGet(t *testing.T, db *DB, key string) string {
	t.Helper()
	var out []byte
	err := db.View(func(tx *Txn) error {
		v, err := tx.Get([]byte(key))
		if err != nil {
			return err
		}
		out = v
		return nil
	})
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	return string(out)
}

func TestBasicCRUD(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	if got := mustGet(t, db, "a"); got != "1" {
		t.Fatalf("got %q, want 1", got)
	}

	// Overwrite.
	mustPut(t, db, "a", "3")
	if got := mustGet(t, db, "a"); got != "3" {
		t.Fatalf("got %q, want 3", got)
	}

	// Delete.
	if err := db.Update(func(tx *Txn) error { return tx.Delete([]byte("a")) }); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	err := db.View(func(tx *Txn) error {
		_, err := tx.Get([]byte("a"))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected ErrKeyNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read-your-own-writes inside a transaction.
	err = db.Update(func(tx *Txn) error {
		if err := tx.Put([]byte("c"), []byte("4")); err != nil {
			return err
		}
		v, err := tx.Get([]byte("c"))
		if err != nil || string(v) != "4" {
			t.Fatalf("read-own-write: got %q, %v", v, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Rollback leaves no trace.
	tx, _ := db.Begin(true)
	tx.Put([]byte("d"), []byte("5"))
	tx.Rollback()
	err = db.View(func(tx *Txn) error {
		_, err := tx.Get([]byte("d"))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("rolled-back key visible: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	mustPut(t, db, "k", "v1")

	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()

	// Concurrent writer overwrites and deletes other keys.
	mustPut(t, db, "k", "v2")
	mustPut(t, db, "x", "9")

	// The read transaction must still see its original snapshot.
	v, err := rtx.Get([]byte("k"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("snapshot read: got %q, %v; want v1", v, err)
	}
	if _, err := rtx.Get([]byte("x")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("key written after snapshot visible: %v", err)
	}

	// A fresh read sees the new value.
	if got := mustGet(t, db, "k"); got != "v2" {
		t.Fatalf("got %q, want v2", got)
	}
}

func TestWriteWriteConflict(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	mustPut(t, db, "k", "base")

	t1, _ := db.Begin(true)
	t2, _ := db.Begin(true)

	if err := t1.Put([]byte("k"), []byte("from-t1")); err != nil {
		t.Fatal(err)
	}
	if err := t2.Put([]byte("k"), []byte("from-t2")); err != nil {
		t.Fatal(err)
	}

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); !errors.Is(err, ErrTxnConflict) {
		t.Fatalf("t2 commit: expected ErrTxnConflict, got %v", err)
	}

	// The losing transaction's write must not be visible.
	if got := mustGet(t, db, "k"); got != "from-t1" {
		t.Fatalf("got %q, want from-t1", got)
	}

	// Disjoint keys do not conflict.
	t3, _ := db.Begin(true)
	t4, _ := db.Begin(true)
	t3.Put([]byte("p"), []byte("1"))
	t4.Put([]byte("q"), []byte("2"))
	if err := t3.Commit(); err != nil {
		t.Fatalf("t3: %v", err)
	}
	if err := t4.Commit(); err != nil {
		t.Fatalf("t4: %v", err)
	}
}

func TestWALRecoveryAfterCrash(t *testing.T) {
	db, dir := openTemp(t)
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("key-%03d", i), fmt.Sprintf("val-%d", i))
	}
	// Simulate a crash: do NOT Close (commits were fsynced already).
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < 100; i++ {
		if got := mustGet(t, db2, fmt.Sprintf("key-%03d", i)); got != fmt.Sprintf("val-%d", i) {
			t.Fatalf("key-%03d: got %q", i, got)
		}
	}
}

func TestRecoveryWithTornTail(t *testing.T) {
	db, dir := openTemp(t)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	db.Close()

	// Corrupt the WAL tail, mimicking a torn write during a crash.
	seqs, err := listWalSegments(dir)
	if err != nil || len(seqs) == 0 {
		t.Fatalf("no WAL segments: %v", err)
	}
	last := walPath(dir, seqs[len(seqs)-1])
	f, err := os.OpenFile(last, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer db2.Close()
	if got := mustGet(t, db2, "a"); got != "1" {
		t.Fatalf("got %q, want 1", got)
	}
	if got := mustGet(t, db2, "b"); got != "2" {
		t.Fatalf("got %q, want 2", got)
	}
}

func TestIteratorSnapshotIsolation(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	for i := 0; i < 10; i++ {
		mustPut(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	rtx, _ := db.Begin(false)
	defer rtx.Rollback()
	it := rtx.NewIterator(nil)
	defer it.Close()

	// Mutate the store after the iterator was created.
	mustPut(t, db, "k0", "changed")
	if err := db.Update(func(tx *Txn) error { return tx.Delete([]byte("k1")) }); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "knew", "new")
	db.Compact()

	// The iterator must still yield the original 10 keys with original values.
	count := 0
	prev := ""
	for it.Rewind(); it.Valid(); it.Next() {
		k, v := string(it.Key()), string(it.Value())
		want := fmt.Sprintf("v%d", count)
		if k != fmt.Sprintf("k%d", count) || v != want {
			t.Fatalf("iter[%d]: got %s=%s, want k%d=%s", count, k, v, count, want)
		}
		if count > 0 && k <= prev {
			t.Fatalf("keys out of order: %q after %q", k, prev)
		}
		prev = k
		count++
	}
	if count != 10 {
		t.Fatalf("iterated %d keys, want 10", count)
	}
}

func TestIteratorSeekAndPrefix(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	for _, k := range []string{"apple", "apricot", "banana", "blueberry"} {
		mustPut(t, db, k, "x")
	}
	err := db.View(func(tx *Txn) error {
		it := tx.NewIterator(&IteratorOptions{Prefix: []byte("a")})
		defer it.Close()
		var got []string
		for it.Rewind(); it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		if len(got) != 2 || got[0] != "apple" || got[1] != "apricot" {
			t.Fatalf("prefix iteration: %v", got)
		}

		it2 := tx.NewIterator(nil)
		defer it2.Close()
		it2.Seek([]byte("apricot"))
		if !it2.Valid() || string(it2.Key()) != "apricot" {
			t.Fatalf("seek apricot: %q", it2.Key())
		}
		it2.Seek([]byte("apx")) // between apricot and banana
		if !it2.Valid() || string(it2.Key()) != "banana" {
			t.Fatalf("seek apx: %q", it2.Key())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCompactionPreservesActiveSnapshots(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	mustPut(t, db, "k", "v1")

	rtx, _ := db.Begin(false)
	defer rtx.Rollback()

	// Overwrite several times, then compact aggressively.
	mustPut(t, db, "k", "v2")
	mustPut(t, db, "k", "v3")
	db.Compact()

	// The old snapshot must still be readable.
	v, err := rtx.Get([]byte("k"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("active snapshot after compaction: got %q, %v; want v1", v, err)
	}
	// New reads see the latest.
	if got := mustGet(t, db, "k"); got != "v3" {
		t.Fatalf("got %q, want v3", got)
	}

	// Once no snapshot needs them, old versions are reclaimed.
	rtx.Rollback()
	db.Compact()
	db.mu.RLock()
	n := len(db.data["k"])
	db.mu.RUnlock()
	if n != 1 {
		t.Fatalf("expected 1 version after compaction, got %d", n)
	}
}

func TestCompactionReclaimsTombstones(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	mustPut(t, db, "dead", "x")
	if err := db.Update(func(tx *Txn) error { return tx.Delete([]byte("dead")) }); err != nil {
		t.Fatal(err)
	}
	db.Compact()
	db.mu.RLock()
	_, exists := db.data["dead"]
	db.mu.RUnlock()
	if exists {
		t.Fatal("tombstoned key should be reclaimed once no snapshot needs it")
	}
}

func TestCheckpointAndWALTruncation(t *testing.T) {
	db, dir := openTemp(t)

	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Old WAL segments must be truncated; only the fresh one remains.
	seqs, err := listWalSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 1 {
		t.Fatalf("expected 1 WAL segment after checkpoint, got %d", len(seqs))
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointFile)); err != nil {
		t.Fatalf("checkpoint file missing: %v", err)
	}

	// More writes after the checkpoint (incremental WAL).
	for i := 50; i < 70; i++ {
		mustPut(t, db, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}
	// Overwrite a checkpointed key too.
	mustPut(t, db, "k00", "updated")
	db.Close()

	// Recovery: checkpoint + incremental replay.
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < 70; i++ {
		want := fmt.Sprintf("v%d", i)
		if i == 0 {
			want = "updated"
		}
		if got := mustGet(t, db2, fmt.Sprintf("k%02d", i)); got != want {
			t.Fatalf("k%02d: got %q, want %q", i, got, want)
		}
	}
}

func TestCheckpointCrashMidway(t *testing.T) {
	db, dir := openTemp(t)
	mustPut(t, db, "a", "1")
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "b", "2")

	// Simulate an interrupted second checkpoint: a stray tmp file exists.
	if err := os.WriteFile(filepath.Join(dir, checkpointTmpFile), []byte("partial garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after interrupted checkpoint: %v", err)
	}
	defer db2.Close()
	if got := mustGet(t, db2, "a"); got != "1" {
		t.Fatalf("got %q, want 1", got)
	}
	if got := mustGet(t, db2, "b"); got != "2" {
		t.Fatalf("got %q, want 2", got)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	db, _ := openTemp(t, WithGCInterval(0)) // manual GC below
	defer db.Close()

	const workers = 8
	const ops = 50

	var wg sync.WaitGroup
	// Writers on disjoint keys: no conflicts expected.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("w%d-k%03d", w, i)
				val := fmt.Sprintf("w%d-v%d", w, i)
				if err := db.Update(func(tx *Txn) error { return tx.Put([]byte(key), []byte(val)) }); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}(w)
	}
	// Concurrent readers and iterators must never observe torn state.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				err := db.View(func(tx *Txn) error {
					it := tx.NewIterator(nil)
					defer it.Close()
					prev := ""
					for it.Rewind(); it.Valid(); it.Next() {
						k := string(it.Key())
						if k <= prev && prev != "" {
							t.Errorf("iterator order violated: %q after %q", k, prev)
						}
						prev = k
					}
					return nil
				})
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
			}
		}()
	}
	// Concurrent compaction must not disturb anyone.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			db.Compact()
		}
	}()
	wg.Wait()

	// Everything the writers committed must be present.
	for w := 0; w < workers; w++ {
		for i := 0; i < ops; i++ {
			key := fmt.Sprintf("w%d-k%03d", w, i)
			want := fmt.Sprintf("w%d-v%d", w, i)
			if got := mustGet(t, db, key); got != want {
				t.Fatalf("%s: got %q, want %q", key, got, want)
			}
		}
	}
}

func TestConcurrentConflictingWriters(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	// Many writers race on the same key; exactly the committed ones may win,
	// losers must get ErrTxnConflict, and the final state must be one of the
	// committed values.
	const racers = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := map[string]bool{}
	for r := 0; r < racers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			val := fmt.Sprintf("racer-%d", r)
			err := db.Update(func(tx *Txn) error { return tx.Put([]byte("hot"), []byte(val)) })
			if err == nil {
				mu.Lock()
				committed[val] = true
				mu.Unlock()
			} else if !errors.Is(err, ErrTxnConflict) {
				t.Errorf("unexpected error: %v", err)
			}
		}(r)
	}
	wg.Wait()

	got := mustGet(t, db, "hot")
	if !committed[got] {
		t.Fatalf("final value %q was never committed", got)
	}
}

func TestBackgroundGC(t *testing.T) {
	db, _ := openTemp(t, WithGCInterval(0)) // interval handled manually via Compact
	defer db.Close()

	mustPut(t, db, "k", "v1")
	mustPut(t, db, "k", "v2")
	db.Compact()
	if got := mustGet(t, db, "k"); got != "v2" {
		t.Fatalf("got %q, want v2", got)
	}
}

func TestCheckpointDuringActiveSnapshot(t *testing.T) {
	db, _ := openTemp(t)

	mustPut(t, db, "k", "v1")
	rtx, _ := db.Begin(false)
	defer rtx.Rollback()

	mustPut(t, db, "k", "v2")
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	db.Compact()

	// Active snapshot still sees its version even after checkpoint + GC.
	v, err := rtx.Get([]byte("k"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("snapshot after checkpoint: got %q, %v; want v1", v, err)
	}
	db.Close()

	// Recovery from the checkpoint yields the latest committed state.
	dir := db.dir
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := mustGet(t, db2, "k"); got != "v2" {
		t.Fatalf("recovered: got %q, want v2", got)
	}
}
