package kvstore

import (
	"bytes"
	"sort"
)

// IteratorOptions configures an iterator.
type IteratorOptions struct {
	// Prefix, if non-empty, restricts iteration to keys with this prefix.
	Prefix []byte
}

// Iterator iterates over a consistent snapshot of the store. The snapshot is
// materialized when the iterator is created, so subsequent writes, deletes,
// and compactions cannot affect it. It is not safe for concurrent use.
type Iterator struct {
	items []iterItem
	idx   int
}

type iterItem struct {
	key []byte
	val []byte
}

// NewIterator creates an iterator over the transaction's snapshot (including
// the transaction's own buffered writes). opts may be nil.
func (t *Txn) NewIterator(opts *IteratorOptions) *Iterator {
	var prefix []byte
	if opts != nil {
		prefix = opts.Prefix
	}
	m := make(map[string][]byte)
	t.db.mu.RLock()
	for k, vs := range t.db.data {
		if v, ok := visibleAt(vs, t.startTS); ok && !v.deleted {
			m[k] = v.val
		}
	}
	t.db.mu.RUnlock()
	if t.update {
		for k, op := range t.writes {
			if op.del {
				delete(m, k)
			} else {
				m[k] = op.val
			}
		}
	}
	items := make([]iterItem, 0, len(m))
	for k, v := range m {
		if len(prefix) > 0 && !bytes.HasPrefix([]byte(k), prefix) {
			continue
		}
		items = append(items, iterItem{
			key: append([]byte(nil), k...),
			val: append([]byte(nil), v...),
		})
	}
	sort.Slice(items, func(i, j int) bool { return bytes.Compare(items[i].key, items[j].key) < 0 })
	return &Iterator{items: items}
}

// Rewind positions the iterator at the first item.
func (it *Iterator) Rewind() { it.idx = 0 }

// Seek positions the iterator at the first item with key >= the given key.
func (it *Iterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, key) >= 0
	})
}

// Valid reports whether the iterator points at an item.
func (it *Iterator) Valid() bool { return it.idx >= 0 && it.idx < len(it.items) }

// Next advances the iterator.
func (it *Iterator) Next() { it.idx++ }

// Key returns the current key. Only valid while Valid() is true.
func (it *Iterator) Key() []byte { return it.items[it.idx].key }

// Value returns the current value. Only valid while Valid() is true.
func (it *Iterator) Value() []byte { return it.items[it.idx].val }

// Close releases the iterator. Iterators hold no locks, so Close is a no-op
// kept for API symmetry and future-proofing.
func (it *Iterator) Close() {}
