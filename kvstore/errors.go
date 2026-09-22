package kvstore

import "errors"

var (
	// ErrKeyNotFound is returned by Get when the key has no visible version.
	ErrKeyNotFound = errors.New("kvstore: key not found")
	// ErrTxnConflict is returned by Commit when another transaction committed
	// a write to the same key after this transaction started (first-committer-wins).
	ErrTxnConflict = errors.New("kvstore: write-write conflict detected")
	// ErrTxnDone is returned when operating on a committed or rolled-back transaction.
	ErrTxnDone = errors.New("kvstore: transaction already finished")
	// ErrReadOnlyTxn is returned when a write is attempted in a read-only transaction.
	ErrReadOnlyTxn = errors.New("kvstore: write in read-only transaction")
	// ErrClosed is returned when operating on a closed database.
	ErrClosed = errors.New("kvstore: database is closed")
)
