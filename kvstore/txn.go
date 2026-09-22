package kvstore

// Txn is a transaction. It is not safe for concurrent use by multiple
// goroutines.
type Txn struct {
	db      *DB
	startTS uint64
	update  bool
	writes  map[string]writeOp
	done    bool
}

// Get returns the value for key as visible in this transaction's snapshot
// (including its own buffered writes), or ErrKeyNotFound.
func (t *Txn) Get(key []byte) ([]byte, error) {
	if t.done {
		return nil, ErrTxnDone
	}
	if t.update {
		if op, ok := t.writes[string(key)]; ok {
			if op.del {
				return nil, ErrKeyNotFound
			}
			return append([]byte(nil), op.val...), nil
		}
	}
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	v, ok := visibleAt(t.db.data[string(key)], t.startTS)
	if !ok || v.deleted {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), v.val...), nil
}

// Put buffers a write of key/value, applied atomically at Commit.
func (t *Txn) Put(key, val []byte) error {
	if t.done {
		return ErrTxnDone
	}
	if !t.update {
		return ErrReadOnlyTxn
	}
	t.writes[string(key)] = writeOp{val: append([]byte(nil), val...)}
	return nil
}

// Delete buffers a deletion of key, applied atomically at Commit.
func (t *Txn) Delete(key []byte) error {
	if t.done {
		return ErrTxnDone
	}
	if !t.update {
		return ErrReadOnlyTxn
	}
	t.writes[string(key)] = writeOp{del: true}
	return nil
}

// Commit validates the transaction against concurrent writers, appends it to
// the WAL, and applies it. On ErrTxnConflict the transaction is rolled back
// and none of its writes become visible.
func (t *Txn) Commit() error {
	if t.done {
		return ErrTxnDone
	}
	t.done = true
	if !t.update {
		t.db.mu.Lock()
		t.db.unregisterLocked(t.startTS)
		t.db.mu.Unlock()
		return nil
	}
	return t.db.commit(t)
}

// Rollback discards the transaction. It is safe to call on an already
// finished transaction.
func (t *Txn) Rollback() {
	if t.done {
		return
	}
	t.done = true
	t.db.mu.Lock()
	t.db.unregisterLocked(t.startTS)
	t.db.mu.Unlock()
}
