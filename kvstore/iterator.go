package kvstore

import "sort"

// Iterator is a consistent snapshot cursor over a key range.
//
// The visible entries are materialized when the iterator is created, so
// the iterator is fully isolated for its whole lifetime: concurrent
// commits, version GC, and checkpoints cannot change or corrupt what it
// returns. The trade-off is memory proportional to the scanned range.
type Iterator struct {
	keys []string
	vals [][]byte
	idx  int
}

// NewIterator creates an iterator over [lower, upper) as seen by this
// transaction (including its own pending writes). An empty lower means
// the beginning; an empty upper means no upper bound.
func (tx *Tx) NewIterator(lower, upper string) (*Iterator, error) {
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.done {
		return nil, ErrTxDone
	}
	m := make(map[string][]byte)
	db.index.iterate(func(key string, n *slNode) bool {
		if v, ok := chainVisible(n.chain, tx.startSeq); ok && !v.tombstone {
			m[key] = append([]byte(nil), v.val...)
		}
		return true
	})
	for k, o := range tx.writes {
		if o.typ == opDel {
			delete(m, k)
		} else {
			m[k] = append([]byte(nil), o.val...)
		}
	}
	it := &Iterator{}
	for k, v := range m {
		if lower != "" && k < lower {
			continue
		}
		if upper != "" && k >= upper {
			continue
		}
		it.keys = append(it.keys, k)
		it.vals = append(it.vals, v)
	}
	sort.Sort(sortKv{it.keys, it.vals})
	return it, nil
}

type sortKv struct {
	keys []string
	vals [][]byte
}

func (s sortKv) Len() int           { return len(s.keys) }
func (s sortKv) Less(i, j int) bool { return s.keys[i] < s.keys[j] }
func (s sortKv) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.vals[i], s.vals[j] = s.vals[j], s.vals[i]
}

// Valid reports whether the iterator points at an entry.
func (it *Iterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

// Key returns the current key. Only valid while Valid is true.
func (it *Iterator) Key() string { return it.keys[it.idx] }

// Value returns the current value. Only valid while Valid is true.
func (it *Iterator) Value() []byte { return it.vals[it.idx] }

// Next advances the iterator.
func (it *Iterator) Next() { it.idx++ }

// Rewind positions the iterator at the first entry.
func (it *Iterator) Rewind() { it.idx = 0 }

// Seek positions the iterator at the first entry with key >= target.
func (it *Iterator) Seek(target string) {
	it.idx = sort.SearchStrings(it.keys, target)
}

// Close releases the iterator. It is a no-op (the snapshot is private to
// the iterator) and exists for API symmetry.
func (it *Iterator) Close() {}
