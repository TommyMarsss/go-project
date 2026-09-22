// Package kvstore is an embedded transactional key-value store using only the
// Go standard library. It provides:
//
//   - WAL-based durability: every committed transaction is appended to the
//     write-ahead log (and fsynced, by default) before it is applied to the
//     in-memory index, so a crash never loses or double-applies committed data.
//   - MVCC: read transactions see a stable snapshot as of their start;
//     write-write conflicts are detected at commit (first-committer-wins) and
//     the losing transaction is rolled back.
//   - Snapshot iterators that stay consistent while writes and compaction run.
//   - Background version GC (compaction) that never reclaims versions still
//     referenced by an active snapshot.
//   - Atomic checkpoints with WAL truncation for fast recovery.
package kvstore

import (
	"os"
	"path/filepath"
	"sync"
	"time"
)

// version is one committed version of a key. Versions of a key are kept
// newest-first (descending ts).
type version struct {
	ts      uint64
	val     []byte
	deleted bool
}

// writeOp is a buffered write inside a transaction.
type writeOp struct {
	val []byte
	del bool
}

// Options configures a DB.
type Options struct {
	// SyncWrites fsyncs the WAL on every commit. Default true. Setting it to
	// false trades durability (recent commits may be lost on crash) for speed.
	SyncWrites bool
	// GCInterval, if > 0, runs version compaction in a background goroutine.
	GCInterval time.Duration
}

// Option mutates Options.
type Option func(*Options)

// WithSyncWrites controls whether commits fsync the WAL (default true).
func WithSyncWrites(sync bool) Option {
	return func(o *Options) { o.SyncWrites = sync }
}

// WithGCInterval enables periodic background version compaction.
func WithGCInterval(d time.Duration) Option {
	return func(o *Options) { o.GCInterval = d }
}

// DB is a transactional key-value store backed by a directory on the local
// filesystem. It is safe for concurrent use.
type DB struct {
	dir  string
	opts Options

	mu sync.RWMutex
	// data maps key -> committed versions, newest first.
	data     map[string][]version
	commitTS uint64
	// active counts live transactions per snapshot timestamp; it is the GC
	// watermark source.
	active map[uint64]int

	walSeq uint64
	wal    *walWriter
	closed bool

	cpMu   sync.Mutex // serializes Checkpoint
	gcStop chan struct{}
	gcDone chan struct{}
}

// Open opens (or creates) a store in dir and recovers state from the latest
// checkpoint plus any WAL segments not covered by it.
func Open(dir string, opts ...Option) (*DB, error) {
	o := Options{SyncWrites: true}
	for _, fn := range opts {
		fn(&o)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Remove leftovers from an interrupted checkpoint; they are never valid.
	os.Remove(filepath.Join(dir, checkpointTmpFile))
	os.Remove(filepath.Join(dir, manifestTmpFile))

	db := &DB{
		dir:    dir,
		opts:   o,
		data:   make(map[string][]version),
		active: make(map[uint64]int),
	}

	mf, err := readManifestFile(dir)
	if err != nil {
		return nil, err
	}

	// 1. Load the checkpoint, if any.
	if mf.CheckpointTS > 0 {
		ts, ents, err := loadCheckpointFile(dir)
		if err != nil {
			return nil, err
		}
		if ts != mf.CheckpointTS {
			// Manifest and checkpoint disagree (crash between the two
			// renames). Trust the checkpoint file itself.
			mf.CheckpointTS = ts
		}
		for _, e := range ents {
			db.data[e.key] = []version{{ts: ts, val: e.val}}
		}
		db.commitTS = ts
	}

	// 2. Replay WAL segments not fully covered by the checkpoint.
	seqs, err := listWalSegments(dir)
	if err != nil {
		return nil, err
	}
	var maxSeq uint64
	for _, seq := range seqs {
		if seq < mf.MinWalSeq {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
		maxTS, err := replayWalSegment(walPath(dir, seq), db.commitTS, func(ts uint64, writes map[string]writeOp) {
			applyWrites(db.data, ts, writes)
		})
		if err != nil {
			return nil, err
		}
		if maxTS > db.commitTS {
			db.commitTS = maxTS
		}
	}

	// 3. Start a fresh WAL segment (never append after a possibly torn tail).
	db.walSeq = maxSeq + 1
	db.wal, err = openWalWriter(db.dir, db.walSeq)
	if err != nil {
		return nil, err
	}

	if o.GCInterval > 0 {
		db.gcStop = make(chan struct{})
		db.gcDone = make(chan struct{})
		go db.gcLoop(o.GCInterval)
	}
	return db, nil
}

// Close stops background compaction and closes the WAL. It does not perform a
// checkpoint; committed data is already durable in the WAL.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.mu.Unlock()

	if db.gcStop != nil {
		close(db.gcStop)
		<-db.gcDone
	}
	return db.wal.close()
}

// Begin starts a transaction. Read-only transactions (update=false) see a
// stable snapshot and never conflict; update transactions buffer writes and
// validate them at Commit.
func (db *DB) Begin(update bool) (*Txn, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	t := &Txn{db: db, startTS: db.commitTS, update: update}
	if update {
		t.writes = make(map[string]writeOp)
	}
	db.active[t.startTS]++
	return t, nil
}

// View runs fn inside a read-only transaction.
func (db *DB) View(fn func(*Txn) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return fn(t)
}

// Update runs fn inside a read-write transaction and commits it. If fn
// returns an error the transaction is rolled back.
func (db *DB) Update(fn func(*Txn) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}
	if err := fn(t); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}

// commit validates, logs, and applies a write transaction.
func (db *DB) commit(t *Txn) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if len(t.writes) == 0 {
		db.unregisterLocked(t.startTS)
		return nil
	}
	// First-committer-wins: conflict if any key we wrote has a committed
	// version newer than our snapshot.
	for key := range t.writes {
		if vs := db.data[key]; len(vs) > 0 && vs[0].ts > t.startTS {
			db.unregisterLocked(t.startTS)
			return ErrTxnConflict
		}
	}
	ts := db.commitTS + 1
	// WAL first, then apply to the in-memory index.
	if err := db.wal.append(encodeRecord(ts, t.writes), db.opts.SyncWrites); err != nil {
		db.unregisterLocked(t.startTS)
		return err
	}
	applyWrites(db.data, ts, t.writes)
	db.commitTS = ts
	db.unregisterLocked(t.startTS)
	return nil
}

func (db *DB) unregisterLocked(startTS uint64) {
	if n := db.active[startTS]; n <= 1 {
		delete(db.active, startTS)
	} else {
		db.active[startTS] = n - 1
	}
}

// applyWrites prepends the new committed versions. Callers must hold db.mu
// (or be in single-threaded recovery).
func applyWrites(data map[string][]version, ts uint64, writes map[string]writeOp) {
	for k, op := range writes {
		data[k] = append([]version{{ts: ts, val: op.val, deleted: op.del}}, data[k]...)
	}
}

// visibleAt returns the newest version visible at readTS.
func visibleAt(vs []version, readTS uint64) (version, bool) {
	for _, v := range vs {
		if v.ts <= readTS {
			return v, true
		}
	}
	return version{}, false
}

// gcWatermark is the oldest snapshot timestamp still referenced by a live
// transaction; versions needed at or below it must be preserved.
func (db *DB) gcWatermarkLocked() uint64 {
	wm := db.commitTS
	for ts := range db.active {
		if ts < wm {
			wm = ts
		}
	}
	return wm
}

// Compact reclaims committed versions that no active or future snapshot can
// observe. It never blocks readers or writers beyond a short critical section
// and never removes a version visible to an active transaction.
func (db *DB) Compact() {
	db.mu.Lock()
	defer db.mu.Unlock()
	wm := db.gcWatermarkLocked()
	for key, vs := range db.data {
		keep := len(vs)
		for i, v := range vs {
			if v.ts <= wm {
				keep = i + 1 // everything older is invisible to everyone
				break
			}
		}
		vs = vs[:keep]
		if len(vs) == 1 && vs[0].deleted {
			// The only version anyone can still see is a tombstone; the key
			// is gone for all practical purposes.
			delete(db.data, key)
			continue
		}
		db.data[key] = vs
	}
}

func (db *DB) gcLoop(interval time.Duration) {
	defer close(db.gcDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.gcStop:
			return
		case <-ticker.C:
			db.Compact()
		}
	}
}

// Checkpoint writes a full snapshot of the committed state, then truncates
// the WAL segments it covers. It is safe to run concurrently with reads and
// writes, and a crash at any point leaves the previous checkpoint and WAL
// intact.
func (db *DB) Checkpoint() error {
	db.cpMu.Lock()
	defer db.cpMu.Unlock()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	// Rotate the WAL first so every record in the sealed segments is covered
	// by the snapshot we are about to take.
	oldWal := db.wal
	db.walSeq++
	w, err := openWalWriter(db.dir, db.walSeq)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	db.wal = w
	cpTS := db.commitTS
	ents := make([]checkpointEntry, 0, len(db.data))
	for k, vs := range db.data {
		if len(vs) > 0 && !vs[0].deleted {
			ents = append(ents, checkpointEntry{key: k, val: vs[0].val})
		}
	}
	newSeq := db.walSeq
	db.mu.Unlock()
	oldWal.close()

	if err := writeCheckpointFile(db.dir, cpTS, ents); err != nil {
		return err
	}
	if err := writeManifestFile(db.dir, manifest{CheckpointTS: cpTS, MinWalSeq: newSeq}); err != nil {
		return err
	}
	// The checkpoint and manifest are durable; covered segments can go.
	seqs, err := listWalSegments(db.dir)
	if err != nil {
		return err
	}
	for _, seq := range seqs {
		if seq < newSeq {
			os.Remove(walPath(db.dir, seq))
		}
	}
	return nil
}
