package kvstore

import "sort"

// Tx is a snapshot-isolation transaction.
//
// Reads see the database as of the transaction's start (plus the
// transaction's own pending writes). A read-write transaction buffers its
// writes; Commit persists them as one WAL record and applies them
// atomically. If another transaction committed a write to any of the same
// keys after this transaction began, Commit fails with ErrConflict and
// the transaction is rolled back — none of its writes become visible.
//
// A Tx is not safe for concurrent use by multiple goroutines.
type Tx struct {
	db       *DB
	startSeq uint64
	readOnly bool
	writes   map[string]op
	done     bool
}

// Get returns the value visible to this transaction, or ErrNotFound.
func (tx *Tx) Get(key string) ([]byte, error) {
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.done {
		return nil, ErrTxDone
	}
	if o, ok := tx.writes[key]; ok {
		if o.typ == opDel {
			return nil, ErrNotFound
		}
		return append([]byte(nil), o.val...), nil
	}
	chain, ok := db.index.get(key)
	if !ok {
		return nil, ErrNotFound
	}
	v, ok := chainVisible(chain, tx.startSeq)
	if !ok || v.tombstone {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v.val...), nil
}

// Put stages a write, visible to this transaction's own reads immediately
// and to others only after Commit.
func (tx *Tx) Put(key string, val []byte) error {
	if tx.readOnly {
		return ErrReadOnly
	}
	if key == "" {
		return ErrEmptyKey
	}
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.done {
		return ErrTxDone
	}
	tx.writes[key] = op{typ: opPut, key: key, val: append([]byte(nil), val...)}
	return nil
}

// Delete stages a deletion.
func (tx *Tx) Delete(key string) error {
	if tx.readOnly {
		return ErrReadOnly
	}
	if key == "" {
		return ErrEmptyKey
	}
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.done {
		return ErrTxDone
	}
	tx.writes[key] = op{typ: opDel, key: key}
	return nil
}

// Commit durably persists the transaction's writes and makes them
// visible to later transactions. It returns ErrConflict (and rolls the
// transaction back) on a write-write conflict.
func (tx *Tx) Commit() error {
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	defer db.releaseSnapshotLocked(tx.startSeq)
	if db.closed {
		return ErrClosed
	}
	if len(tx.writes) == 0 {
		return nil
	}
	// First-committer-wins: abort if any key we wrote has a committed
	// version newer than our snapshot.
	for key := range tx.writes {
		if chain, ok := db.index.get(key); ok {
			if chain[len(chain)-1].seq > tx.startSeq {
				return ErrConflict
			}
		}
	}
	ops := make([]op, 0, len(tx.writes))
	for _, o := range tx.writes {
		ops = append(ops, o)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].key < ops[j].key })
	// WAL first, then apply to the in-memory index.
	rec := encodeRecord(db.seq+1, ops)
	if err := db.wal.append(rec); err != nil {
		return err
	}
	if db.opts.SyncWrites {
		if err := db.wal.sync(); err != nil {
			return err
		}
	}
	db.seq++
	db.applyOps(ops, db.seq)
	return nil
}

// Rollback discards the transaction. It is idempotent and safe to defer.
func (tx *Tx) Rollback() error {
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.done {
		return nil
	}
	tx.done = true
	db.releaseSnapshotLocked(tx.startSeq)
	return nil
}
