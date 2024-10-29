package main

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
)

type nodeType uint8

const (
	ROOT_NODE nodeType = iota + 1
	INTERNAL_NODE
	LEAF_NODE
)

type BTree struct {
	root      *node
	nodeCount int
	maxDegree int

	db *DB
	mu sync.RWMutex
}

type node struct {
	kind   nodeType
	parent *node // only accessible to leaf nodes
	// Each entry in this table b-tree consists of a 64-bit signed integer
	// key and up to 2147483647 bytes of arbitrary data.
	// In RocksDB for e.g k/v are arbitrary byte sequences
	keys     []int
	children []*node
	data     []int

	// sibling pointers
	next     *node
	previous *node

	// dir index
	pageId int64
}

// degree relates to number of children = maxKeys + 1
// which relates to the branching factor (bound on children)
// branching factor can be expressed as maxDegree, and is the inequality
// b - 1 <= num keys < (2 * b) - 1
func NewBTree(maxDegree int) *BTree {
	// invariant one
	_assert(maxDegree >= 2, "the minimum maxDegree of a B+ tree must be greater than 2")

	// root node is initially empty and triggers initial/startup page allocations.
	// assumes the db file is truncated and the init pageSize is at seek 0
	return &BTree{
		root: &node{
			kind:     ROOT_NODE,
			keys:     []int{},
			children: []*node{},
			next:     nil,
			previous: nil,
			parent:   nil,
			pageId:   0,
		},
		maxDegree: maxDegree,
	}
}

// Search starts from the root and traverses all internal nodes until it finds
// the leaf node containing the right page and ccesses it.
func (t *BTree) Get(key int) ([]int, int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil, 0, errors.New("empty tree")
	} else {
		node, idx, _ := t.root.search(key)

		return node.data, idx, nil
	}
}

func (t *BTree) Upsert(key int, value int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.root == nil {
		t.root = &node{kind: ROOT_NODE}
		t.root.insert(t, key)

		t.nodeCount++
		return nil
	} else {
		// find leaf node to Upsert into or root at first
		n, _, err := t.root.search(key)

		if n == nil {
			return fmt.Errorf("leaf node not found: %v", err)
		}

		t.nodeCount++
		return n.insert(t, key)
	}
}

func (n *node) search(key int) (*node, int, error) {
	idx, found := slices.BinarySearch(n.keys, key)

	if found {
		if len(n.children) == 0 {
			/*
				// Binary search for the key
					if node.kind == LEAF_NODE || node.kind == INTERNAL_NODE {
						start, end := 0, len(node.keys)-1

						for start <= end {
							mid := start + (end-start)/2
							if node.keys[mid] == key {
								// Calculate the offset and seek to it
								offset := int64(binary.LittleEndian.Uint64([]byte(strconv.Itoa(node.keys[mid]))))
								if _, err := t.db.datafile.Seek(offset, io.SeekStart); err != nil {
									return nil, err
								}

								// TODO: factor out when using binary format
								// read to delimiter
								value, err := reader.ReadBytes('\n')
								if err != nil {
									return nil, err
								}

								return value, nil
							} else if node.keys[mid] < key {
								start = mid + 1
							} else {
								end = mid - 1
							}
						}
						return nil, errors.New("key not found")
					}

			*/
			return n, idx, nil
		} else {
			return n.children[idx].search(key)
		}
	}

	if len(n.children) == 0 {
		return n, 0, errors.New("key not found, at leaf containing key")
	}

	if idx >= len(n.children) {
		return n.children[idx-1].search(key)
	}

	return n.children[idx].search(key)
}

func (n *node) insert(t *BTree, key int) error {
	if n.kind == ROOT_NODE && len(n.children) == 0 {
		n.data = findInsertAt(n.data, key)
		n.keys = findInsertAt(n.keys, key)
	}

	if n.kind == LEAF_NODE {
		n.data = findInsertAt(n.data, key)
	}

	if len(n.data) < t.maxDegree {
		return nil
	} else {

		n.split(t, len(n.data)/2)
	}

	return nil
}

func (n *node) split(t *BTree, midIdx int) error {
	switch n.kind {
	case LEAF_NODE:
		splitPoint := n.data[midIdx]
		left, right := n.data[:midIdx], n.data[midIdx:]
		n.data = left

		newNode := &node{kind: LEAF_NODE, parent: n.parent, data: right}

		n.parent.children = append(n.parent.children, newNode)
		n.parent.keys = findInsertAt(n.parent.keys, splitPoint)

		// sibling pointers - only on leaf nodes
		n.next = newNode
		newNode.previous = n

	case INTERNAL_NODE:
		splitPoint := n.keys[midIdx]

		// NB: note it's index/key + 1 for internal
		left, right := n.keys[:midIdx], n.keys[midIdx+1:]
		n.keys = left

		newNode := &node{kind: INTERNAL_NODE, keys: right, parent: n.parent}
		n.parent.children = append(n.parent.children, newNode)
		n.parent.keys = findInsertAt(n.parent.keys, splitPoint)

		// pointer relocation/bookkeeping
		mid := len(n.children) / 2
		leftPointers, rightPointers := n.children[:mid], n.children[mid:]

		for _, child := range rightPointers {
			child.parent = newNode
		}

		n.children, newNode.children = leftPointers, rightPointers

	case ROOT_NODE:
		if len(n.data) == 0 {
			splitPoint := n.keys[midIdx]
			left, right := n.keys[:midIdx], n.keys[midIdx+1:]

			// demote current root
			newRoot := &node{kind: ROOT_NODE, parent: nil}
			newRoot.keys = findInsertAt(newRoot.keys, splitPoint)
			t.root = newRoot

			// pointer relocation/bookkeeping
			mid := len(n.children) / 2
			leftPointers, rightPointers := n.children[:mid], n.children[mid:]
			sibling := &node{kind: INTERNAL_NODE, keys: left, children: leftPointers, parent: newRoot}
			n.kind, n.keys, n.children, n.parent = INTERNAL_NODE, right, rightPointers, newRoot
			newRoot.children = append(newRoot.children, sibling, n)

			for _, child := range leftPointers {
				child.parent = sibling
			}

		} else {
			// demote current root to a leaf
			n.keys = []int{}
			n.kind = LEAF_NODE
			newRoot := &node{kind: ROOT_NODE, parent: nil}
			n.parent = newRoot
			t.root = newRoot

			newRoot.children = append(newRoot.children, n)

			n.split(t, len(n.data)/2)
		}

	}

	if len(n.parent.keys) > t.maxDegree-1 {
		n.parent.split(t, len(n.parent.keys)/2)
	}

	return nil
}

func (t *BTree) Delete(key int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.root == nil {
		return errors.New("empty tree")
	} else {
		// find leaf node to delete from or root
		n, _, err := t.root.searchDel(key)

		if err == nil {
			t.nodeCount--
			return n.delete(t, key)
		}

		return errors.New("key not in tree")
	}
}

// TODO(refactor): collapse this function into one
func (n *node) searchDel(key int) (*node, int, error) {
	idx, found := slices.BinarySearch(n.keys, key)

	if found {
		if n.kind == LEAF_NODE {
			return n, idx, nil
		} else {
			if len(n.children) == 0 {
				return n, 0, nil
			}

			if idx+1 > len(n.children) {
				return n.children[idx].searchDel(key)
			}

			return n.children[idx+1].searchDel(key)
		}
	}

	if len(n.children) == 0 {
		return n, 0, nil
	}

	return n.children[idx].searchDel(key)
}

// Deletion is the most complicated operation for a B-Tree.
// this covers part one, "merging"
// see: https://opendatastructures.org/ods-python/14_2_B_Trees.html#SECTION001723000000000000000
func (n *node) delete(t *BTree, key int) error {
	for i, v := range n.data {
		if v == key {
			n.data = cut(i, n.data)
		}
	}

	if n.kind == ROOT_NODE {
		return nil
	}

	// is the leaf empty or underflown?
	if n.kind == LEAF_NODE && len(n.data) < (t.maxDegree/2) {
		if sibling, _, err := n.preMerge(t.maxDegree); err == nil {
			return n.mergeSibling(t, sibling, key)
		} else {
			return errors.New("see rebalancing.go")
		}
	} else {
		// should we update the parent's separator?
		if n.parent.keys[0] < n.data[0] {
			// delete the key from the parent
			for i, k := range n.parent.keys {
				if k == key {
					n.parent.keys = cut(i, n.parent.keys)
					newSeperator := len(n.data) / 2
					n.parent.keys = append(n.parent.keys, n.data[newSeperator])
				}
			}
		}
	}

	// underflow triggers a merge cascade recurse to parent
	// recurse UPWARD and check invariants
	if len(n.parent.keys) < ((t.maxDegree - 1) / 2) {
		if sibling, _, err := n.parent.preMerge(t.maxDegree); err == nil {
			return n.parent.mergeSibling(t, sibling, key)
		} else {
			return errors.New("see rebalancing.go")
		}
	}
	return nil
}

// preMerge if two adjacent leaf nodes have a common parent and their contents fit into a single node
func (n *node) preMerge(size int) (*node, int, error) {
	switch n.kind {
	case INTERNAL_NODE:
		// no sibling pointers so we have to go up to parent
		// we check all our siblings if we can re-distribute

		for i, sibling := range n.parent.children {
			if n == sibling {
				// cannot merge with self
				continue
			} else {
				// can merge with sibling?
				if len(sibling.keys)+len(n.keys) < size {
					return sibling, i, nil

				}
			}
		}

	case LEAF_NODE:
		if n.previous != nil {
			if len(n.previous.data)+len(n.data) < size {
				n.previous.next = n.next
				return n.previous, 0, nil
			}
		}

		if n.next != nil {
			if len(n.next.data)+len(n.data) < size {
				n.next.previous = n.previous
				return n.next, 0, nil
			}
		}

	case ROOT_NODE:
		// if underfull merge with first left child
		if len(n.keys)+len(n.children[0].keys) <= size {
			return n.children[0], 0, nil
		}
	}

	return nil, 0, errors.New("cannot merge with sibling")
}

// merging can be... very interesting.
// you can slap on an iter api like(rust):
// https://github.com/rust-lang/rust/blob/1c19595575968ea77c7f85e97c67d44d8c0f9a68/library/alloc/src/collections/btree/merge_iter.rs#L41

// go/pebble
// iterator/cursor: https://github.com/cockroachdb/pebble/blob/c4daad9128e053e496fa7916fda8b6df57256823/internal/manifest/btree.go#L973 &&
// https://github.com/cockroachdb/pebble/blob/c4daad9128e053e496fa7916fda8b6df57256823/internal/manifest/btree.go#L891

// the actual merge operation
// https://github.com/cockroachdb/pebble/blob/c4daad9128e053e496fa7916fda8b6df57256823/internal/manifest/btree.go#L620
func (n *node) mergeSibling(t *BTree, sibling *node, key int) error {
	switch n.kind {
	case LEAF_NODE:
		_assert(n.parent == sibling.parent, "non-common ancestor")
		sibling.data = append(sibling.data, n.data...)

		// deallocate/collapse underflow node
		for i, node := range sibling.parent.children {
			if node == n {
				n.parent.children = append(n.parent.children[:i], n.parent.children[i+1:]...)
			}
		}

		for i, k := range sibling.parent.keys {
			if k == key {
				sibling.parent.keys = cut(i, sibling.parent.keys)

				if len(n.parent.keys) < int(math.Ceil(float64(t.maxDegree)/2)) {
					if sibling, _, err := sibling.parent.preMerge(t.maxDegree); err == nil {
						return n.parent.mergeSibling(t, sibling, key)
					} else {
						return errors.New("see rebalancing.go")
					}
				}
			}
		}

	case INTERNAL_NODE:
		_assert(n.parent == sibling.parent, "non-common ancestor")
		sibling.keys = append(sibling.keys, n.keys...)
		sibling.children = append(sibling.children, n.children...)

		// mark n for deallocation
		for i, child := range n.parent.children {
			if child == n {
				n.parent.children = append(n.parent.children[:i], n.parent.children[i+1:]...)
				break
			}
		}

		// recursive case
		if len(n.parent.children) < int(math.Ceil(float64(t.maxDegree)/2)) {
			if sibling, _, err := n.parent.preMerge(t.maxDegree); err == nil {
				return n.parent.mergeSibling(t, sibling, key)
			} else {
				return errors.New("see rebalancing.go")
			}
		}
	case ROOT_NODE:
		sibling.keys = append(sibling.keys, n.keys...)
		sibling.kind = ROOT_NODE
		t.root = sibling
	}

	return nil
}

// finds the offset in the page and writes to it
func findInsertAt(elems []int, elem int) []int {
	if len(elems) == 0 {
		return append(elems, elem)
	}

	idx := sort.Search(len(elems), func(i int) bool {
		return elems[i] >= elem
	})

	/*
		// for testing todo: remove
		// t.db.datafile.Seek(v.pageId, io.SeekStart)
		result := make([]byte, 5)
		t.db.datafile.Seek(5, io.SeekStart)
		t.db.datafile.Read(result)

	*/

	elems = append(elems, 0)
	copy(elems[idx+1:], elems[idx:])
	elems[idx] = elem

	return elems
}

func cut(idx int, elems []int) []int {
	if len(elems) == 1 {
		return nil
	} else {
		return append(elems[:idx], elems[idx+1:]...)
	}
}
