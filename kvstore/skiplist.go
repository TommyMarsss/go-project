package kvstore

import (
	"math/rand"
	"sort"
	"time"
)

// version is one MVCC version of a key. Versions in a chain are kept in
// ascending seq order; the last element is the newest.
type version struct {
	seq       uint64
	val       []byte
	tombstone bool
}

// chainVisible returns the newest version with seq <= snap.
func chainVisible(chain []version, snap uint64) (version, bool) {
	i := sort.Search(len(chain), func(i int) bool { return chain[i].seq > snap }) - 1
	if i < 0 {
		return version{}, false
	}
	return chain[i], true
}

const slMaxLevel = 20

type slNode struct {
	key   string
	chain []version // ascending by seq
	next  []*slNode
}

// skiplist is an ordered in-memory index mapping key -> version chain.
// It is not goroutine-safe; all access is serialized by DB.mu.
type skiplist struct {
	head  *slNode
	level int
	rnd   *rand.Rand
	size  int
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &slNode{next: make([]*slNode, slMaxLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *skiplist) randomLevel() int {
	lvl := 1
	for lvl < slMaxLevel && s.rnd.Intn(2) == 1 {
		lvl++
	}
	return lvl
}

func (s *skiplist) findNode(key string) *slNode {
	n := s.head
	for i := s.level - 1; i >= 0; i-- {
		for n.next[i] != nil && n.next[i].key < key {
			n = n.next[i]
		}
	}
	n = n.next[0]
	if n != nil && n.key == key {
		return n
	}
	return nil
}

// get returns the version chain for key, or nil if the key is absent.
func (s *skiplist) get(key string) ([]version, bool) {
	n := s.findNode(key)
	if n == nil {
		return nil, false
	}
	return n.chain, true
}

// addVersion appends v to the chain of key, creating the node if needed.
// Callers must append versions in ascending seq order.
func (s *skiplist) addVersion(key string, v version) {
	update := make([]*slNode, slMaxLevel)
	n := s.head
	for i := s.level - 1; i >= 0; i-- {
		for n.next[i] != nil && n.next[i].key < key {
			n = n.next[i]
		}
		update[i] = n
	}
	next := n.next[0]
	if next != nil && next.key == key {
		next.chain = append(next.chain, v)
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	nn := &slNode{key: key, chain: []version{v}, next: make([]*slNode, lvl)}
	for i := 0; i < lvl; i++ {
		nn.next[i] = update[i].next[i]
		update[i].next[i] = nn
	}
	s.size++
}

// setChain replaces the chain of an existing node (used by version GC).
func (s *skiplist) setChain(key string, chain []version) {
	if n := s.findNode(key); n != nil {
		n.chain = chain
	}
}

func (s *skiplist) delete(key string) {
	update := make([]*slNode, slMaxLevel)
	n := s.head
	for i := s.level - 1; i >= 0; i-- {
		for n.next[i] != nil && n.next[i].key < key {
			n = n.next[i]
		}
		update[i] = n
	}
	n = n.next[0]
	if n == nil || n.key != key {
		return
	}
	for i := 0; i < s.level; i++ {
		if update[i].next[i] == n {
			update[i].next[i] = n.next[i]
		}
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
	s.size--
}

// iterate visits nodes in ascending key order. The callback receives the
// node itself so compaction may replace its chain in place; it must not
// delete nodes (deletions are collected and applied after iteration).
func (s *skiplist) iterate(fn func(key string, n *slNode) bool) {
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		if !fn(n.key, n) {
			return
		}
	}
}
