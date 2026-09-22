package kvstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	// ErrNotFound is returned by Get when the key has no visible live version.
	ErrNotFound = errors.New("kvstore: key not found")
	// ErrConflict is returned by Commit when another transaction committed a
	// write to one of this transaction's keys after this transaction began.
	// The transaction is rolled back and none of its writes become visible.
	ErrConflict = errors.New("kvstore: write conflict, transaction rolled back")
	// ErrTxDone is returned when using a transaction after Commit/Rollback.
	ErrTxDone = errors.New("kvstore: transaction already finished")
	// ErrReadOnly is returned when writing through a read-only transaction.
	ErrReadOnly = errors.New("kvstore: write on read-only transaction")
	// ErrClosed is returned when using a closed database.
	ErrClosed = errors.New("kvstore: database is closed")
	// ErrEmptyKey is returned for empty keys.
	ErrEmptyKey = errors.New("kvstore: empty key")
)

const walFileName = "wal.log"

// Options configures a DB. Passing nil to Open uses DefaultOptions.
// A non-nil Options is used verbatim: zero GCInterval disables background
// version GC, zero CheckpointBytes disables auto-checkpointing, and
// SyncWrites=false skips fsync on commit (faster, but a power loss may
// lose recently committed transactions).
type Options struct {
	// SyncWrites fsyncs the WAL before a commit is acknowledged.
	SyncWrites bool
	// GCInterval is how often obsolete in-memory versions are reclaimed.
	GCInterval time.Duration
	// CheckpointBytes triggers an automatic checkpoint (and WAL
	// truncation) once the WAL grows past this many bytes.
	CheckpointBytes int64
}

// DefaultOptions returns the options used when Open is called with nil.
func DefaultOptions() Options {
	return Options{
		SyncWrites:      true,
		GCInterval:      time.Minute,
		CheckpointBytes: 64 << 20,
	}
}

// DB is an embedded MVCC key-value store backed by a WAL, periodic
// checkpoints, and an in-memory ordered index.
type DB struct {
	dir  string
	opts Options

	mu        sync.Mutex
	index     *skiplist
	seq       uint64 // last committed sequence number
	wal       *wal
	snapshots map[uint64]int // active snapshot seq -> refcount

	closed bool
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Open opens (or creates) a store in dir and recovers committed state:
// it loads the newest checkpoint, then replays committed WAL records with
// a higher sequence number. A torn or corrupt WAL tail from a crash is
// truncated; a leftover checkpoint.tmp from an interrupted checkpoint is
// discarded.
func Open(dir string, opts *Options) (*DB, error) {
	o := DefaultOptions()
	if opts != nil {
		o = *opts
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// A leftover tmp checkpoint means a checkpoint was interrupted; the
	// previous checkpoint.dat (if any) is still intact. Discard the tmp.
	_ = os.Remove(filepath.Join(dir, ckptTmpName))

	db := &DB{
		dir:       dir,
		opts:      o,
		index:     newSkiplist(),
		snapshots: make(map[uint64]int),
		stopCh:    make(chan struct{}),
	}

	// 1. Load checkpoint, if any.
	ckptPath := filepath.Join(dir, ckptName)
	var ckptSeq uint64
	if _, err := os.Stat(ckptPath); err == nil {
		seq, entries, err := readCheckpoint(ckptPath)
		if err != nil {
			return nil, fmt.Errorf("kvstore: %w", err)
		}
		ckptSeq = seq
		for _, e := range entries {
			db.index.addVersion(e.key, version{seq: seq, val: e.val})
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	db.seq = ckptSeq

	// 2. Replay WAL records newer than the checkpoint.
	walPath := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	validLen, err := replayWAL(f, ckptSeq, func(seq uint64, ops []op) error {
		db.applyOps(ops, seq)
		if seq > db.seq {
			db.seq = seq
		}
		return nil
	})
	if err != nil {
		f.Close()
		return nil, err
	}
	// Truncate a possibly torn tail, then append from the clean end.
	if err := f.Truncate(validLen); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	db.wal = &wal{f: f, size: validLen}

	// 3. Background maintenance (version GC + auto-checkpoint).
	if o.GCInterval > 0 || o.CheckpointBytes > 0 {
		db.wg.Add(1)
		go db.background()
	}
	return db, nil
}

// Close stops background maintenance, flushes and closes the WAL. It does
// not checkpoint; reopening replays the WAL.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.mu.Unlock()

	close(db.stopCh)
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.SyncWrites {
		_ = db.wal.sync()
	}
	return db.wal.close()
}

// applyOps applies a committed transaction's ops at the given seq.
// Callers must hold db.mu and apply in ascending seq order.
func (db *DB) applyOps(ops []op, seq uint64) {
	for _, o := range ops {
		switch o.typ {
		case opPut:
			db.index.addVersion(o.key, version{seq: seq, val: o.val})
		case opDel:
			db.index.addVersion(o.key, version{seq: seq, tombstone: true})
		}
	}
}

// Begin starts a transaction. Read-only transactions never write the WAL
// and never conflict; read-write transactions buffer writes and commit
// them atomically with first-committer-wins conflict detection.
func (db *DB) Begin(readOnly bool) (*Tx, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	db.snapshots[db.seq]++
	return &Tx{
		db:       db,
		startSeq: db.seq,
		readOnly: readOnly,
		writes:   make(map[string]op),
	}, nil
}

// releaseSnapshotLocked drops one reference to a snapshot seq.
// Callers must hold db.mu.
func (db *DB) releaseSnapshotLocked(seq uint64) {
	if n := db.snapshots[seq]; n <= 1 {
		delete(db.snapshots, seq)
	} else {
		db.snapshots[seq] = n - 1
	}
}

// minSnapshotLocked returns the oldest seq any live transaction or
// iterator may still read, or db.seq when none are active.
func (db *DB) minSnapshotLocked() (uint64, bool) {
	min := db.seq
	active := false
	for s := range db.snapshots {
		if !active || s < min {
			min = s
			active = true
		}
	}
	return min, active
}

// Get is a shorthand single-read transaction.
func (db *DB) Get(key string) ([]byte, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return tx.Get(key)
}

// Put is a shorthand single-write transaction. It cannot conflict.
func (db *DB) Put(key string, val []byte) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	if err := tx.Put(key, val); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Delete is a shorthand single-write transaction. It cannot conflict.
func (db *DB) Delete(key string) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	if err := tx.Delete(key); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Checkpoint writes a consistent snapshot of all live versions to
// checkpoint.dat (via tmp file + atomic rename) and then truncates the
// WAL. It is safe to crash at any point: recovery uses the previous
// checkpoint plus the full WAL, or the new checkpoint plus an empty WAL.
// Checkpoint serializes with commits for its duration.
func (db *DB) Checkpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	entries := make([]ckptEntry, 0, db.index.size)
	db.index.iterate(func(key string, n *slNode) bool {
		latest := n.chain[len(n.chain)-1]
		if !latest.tombstone {
			entries = append(entries, ckptEntry{key: key, val: append([]byte(nil), latest.val...)})
		}
		return true
	})
	if err := writeCheckpoint(db.dir, db.seq, entries); err != nil {
		return err
	}
	// The checkpoint is durable; everything in the WAL is now covered.
	if err := db.wal.close(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(db.dir, walFileName), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	db.wal = &wal{f: f}
	return syncDir(db.dir)
}

// Compact reclaims in-memory versions that no active snapshot can see.
// It never blocks or invalidates live transactions: versions visible to
// the oldest active snapshot are retained. Tombstones are removed
// entirely once no snapshot can observe them.
func (db *DB) Compact() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.gcVersionsLocked()
}

func (db *DB) gcVersionsLocked() {
	minSnap, active := db.minSnapshotLocked()
	var toDelete []string
	db.index.iterate(func(key string, n *slNode) bool {
		// i is the index of the newest version visible at minSnap.
		i := sort.Search(len(n.chain), func(i int) bool { return n.chain[i].seq > minSnap }) - 1
		if i > 0 {
			rest := make([]version, len(n.chain)-i)
			copy(rest, n.chain[i:])
			n.chain = rest
		}
		// With no active snapshots the newest version is the only one
		// anyone can see; if it is a tombstone the key can vanish.
		if !active && n.chain[len(n.chain)-1].tombstone {
			toDelete = append(toDelete, key)
		}
		return true
	})
	for _, key := range toDelete {
		db.index.delete(key)
	}
}

func (db *DB) background() {
	defer db.wg.Done()
	interval := db.opts.GCInterval
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-db.stopCh:
			return
		case <-t.C:
			db.mu.Lock()
			closed := db.closed
			if !closed {
				db.gcVersionsLocked()
			}
			needCkpt := !closed && db.opts.CheckpointBytes > 0 && db.wal.size >= db.opts.CheckpointBytes
			db.mu.Unlock()
			if needCkpt {
				_ = db.Checkpoint()
			}
		}
	}
}
