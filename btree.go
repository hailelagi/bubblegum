package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
)

type nodeType int

// oh go, where art thy sum types? thine enums forsake me :(
const (
	ROOT_NODE nodeType = iota + 1
	INTERNAL_NODE
	LEAF_NODE
)

// invariant three: relationship between keys and child pointers
// every node (ie leaf or internal) except the root must have:
// at least MIN_DEGREE children
// todo: _assert this in split/merge
var MIN_DEGREE_NODE int

type BTree struct {
	root      *node
	maxDegree int

	db *DB
	sync.RWMutex
}

type node struct {
	kind     nodeType
	pageId   int64
	keys     []int
	next     *node
	previous *node
	parent   *node
	children []*node
}

// degree relates to number of children = maxKeys + 1
// which relates to the branching factor (bound on children)
// branching factor can be expressed as maxDegree, and is the inequality
// b - 1 <= num keys < (2 * b) - 1
func NewBTree(maxDegree int) *BTree {
	// invariant one
	_assert(maxDegree >= 2, "the minimum maxDegree of a B+ tree must be greater than 2")

	// root node is initially empty and triggers no page allocation.
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

// Insert inserts a key/value pair into the B-tree
func (t *BTree) Insert(key int, value []byte) error {
	t.Lock()
	defer t.Unlock()

	_, err := findNode(t.root, key)

	if err == nil {
		return errors.New("attempted to insert duplicate key")
	} else {
		return t.root.insert(t, key, value, t.maxDegree)
	}
}

// Scan traverses all the nodes in a B-tree in linear time.
// it starts off at the left most pointer and recursively does
// an inorder traversal to all leaf nodes.
// may not implement
func (t *BTree) Scan() ([][]byte, error) {
	return nil, errors.New("unimplemented")
}

func (t *BTree) Range(start, end int) ([][]byte, error) {
	return nil, nil
}

// Search starts from the root and traverses all internal nodes until it finds
// the leaf node containing key, accesses its page and returns the byte arrary with the key/value.
func (t *BTree) Get(key int) ([]byte, error) {
	t.RLock()
	defer t.RUnlock()

	_, err := findNode(t.root, key)

	if err != nil {
		return nil, err
	}

	// for testing todo: remove
	// t.db.datafile.Seek(v.pageId, io.SeekStart)
	result := make([]byte, 5)
	t.db.datafile.Seek(5, io.SeekStart)
	t.db.datafile.Read(result)

	return result, nil

}

func (t *BTree) Delete(key int) error {
	t.Lock()
	defer t.Unlock()

	n, err := findNode(t.root, key)

	if err == nil {
		return n.delete(t, key, t.maxDegree)
	} else {
		return err
	}
}

// TODO: for now the assumption is the key == offset, this is not true.
// findNode searches from the root and traverses all internal nodes until it finds
// the leaf node containing key or in the case of a single node root, the root.
func findNode(root *node, key int) (*node, error) {
	currNode := root

	if len(currNode.keys) == 0 {
		return nil, errors.New("key not found")
	}

	if len(currNode.keys) == 1 {
		if currNode.keys[0] == key {
			return currNode, nil
		} else {
			return nil, errors.New("key not found")
		}
	}

	start, end := 0, len(currNode.keys)-1

	for start <= end {
		mid := start + (end-start)/2

		if currNode.keys[mid] == key {
			return currNode, nil
		} else if currNode.keys[mid] > key {
			end = mid - 1
		} else {
			start = mid + 1
		}
	}

	search := start + (end-start)/2

	if len(currNode.children) == 0 || currNode.kind == LEAF_NODE {
		return nil, errors.New("key not found")
	} else {
		// validate relationship btwn num keys and searchIndex
		// but should work
		return findNode(currNode.children[search], key)
	}
}

// invariant two: relationship between keys and child pointers
// Each node holds up to N keys and N + 1 pointers to the child nodes
func (n *node) insert(t *BTree, key int, value []byte, degree int) error {
	switch n.kind {
	case ROOT_NODE:
		if len(n.keys) > degree-1 {
			n.splitChild(t, len(n.keys)/2, t.maxDegree)
		} else {
			offset, err := syncToOffset(t.db.datafile, value)
			if err != nil {
				return err
			}
			n.pageId = offset

			// TODO: this key thing
			// do you map offsets to the id directly?
			n.keys = append(n.keys, int(offset))
		}
	case LEAF_NODE:
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}

		offset, err := syncToOffset(t.db.datafile, value)

		if err != nil {
			return err
		}

		n.keys = append(n.keys, int(offset))

		copy(n.keys[i+1:], n.keys[i:])
		n.keys[i] = key
	case INTERNAL_NODE:
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}
		n.children[i].insert(t, key, value, t.maxDegree)
	}

	return nil
}

func (node *node) search(t *BTree, key int) ([]byte, error) {
	reader := bufio.NewReader(t.db.datafile)

	if node.kind == ROOT_NODE {
		for _, k := range node.keys {
			if k == key {
				offset := int64(binary.LittleEndian.Uint64([]byte(strconv.Itoa(k))))
				if _, err := t.db.datafile.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				value, err := reader.ReadBytes('\n')

				if err != nil {
					return value, nil
				}
			}
		}
	}

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

	// If the node is not a leaf node, recursively search in the appropriate child
	i := 0
	for i < len(node.keys) && key >= node.keys[i] {
		i++
	}

	return node.children[i].search(t, key) // Recursively search in child node
}

func (node *node) delete(t *BTree, key int, maxDegree int) error {
	// find node using key
	// pass in node, to node stealSibling
	// assume node is root at first
	// this an optimisation maybe do later
	ok, err := node.stealSibling(t, maxDegree)

	if err != nil {
		return err
	}

	if !ok {
		err := node.mergeChildren(t, maxDegree)

		if err != nil {
			return err
		}
	}

	return nil
}

func (node *node) stealSibling(t *BTree, maxDegree int) (bool, error) {
	return false, nil
}

func (node *node) mergeChildren(t *BTree, maxDegree int) error {
	return nil
}

// splitChild splits the child n of the current n at the specified index.
func (n *node) splitChild(t *BTree, index, maxDegree int) {
	// Create a new n to hold the keys and children that will be moved
	newNode := &node{
		keys:     make([]int, 0),
		children: make([]*node, 0),
		kind:     LEAF_NODE,
		next:     n.next,
	}

	// TODO: this is.. not correct.
	// Move keys and children to the new n
	copy(newNode.keys, n.keys[index:])
	copy(newNode.children, n.children[index:])
	n.keys = n.keys[:index]
	n.children = n.children[:index]

	// TODO(FIXME): next is not set correctly
	if n.kind == LEAF_NODE {
		n.next = newNode
	}

	// If the current n is the root, create a new root and add the median key
	if n.kind == ROOT_NODE {
		newRoot := &node{
			keys:     []int{newNode.keys[0]},
			children: []*node{n, newNode},
			kind:     ROOT_NODE,
		}
		t.root = newRoot
		return
	}

	// Otherwise, insert the median key into the parent n
	parent := n.parent
	parent.keys = append(parent.keys, 0)
	parent.children = append(parent.children, nil)
	copy(parent.keys[index+1:], parent.keys[index:])
	copy(parent.children[index+1:], parent.children[index:])
	parent.keys[index] = newNode.keys[0]
	parent.children[index] = newNode

	// Check if the parent n needs splitting
	if len(parent.keys) > maxDegree {
		parent.splitChild(t, len(parent.keys)/2, maxDegree)
	}
}

func syncToOffset(file *os.File, value []byte) (int64, error) {
	writer := bufio.NewWriter(file)
	// TODO: sync this to use the new page layout
	// seek to correct block position using the pageID
	// make sure we get to disk
	_, err := writer.Write(value)
	fErr := writer.Flush()

	if err != nil {
		return 0, err
	}

	if fErr != nil {
		return 0, fErr
	}

	offset, err := file.Seek(0, io.SeekCurrent)

	if err != nil {
		return 0, err
	}

	return offset, nil
}
