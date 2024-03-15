/*
A 'simple' Persistent/On-Disk B Plus Tree.
node keys are assumed to be signed integers and values a slice of bytes.
Persistence is achieved using a naive IO buffer managed by the OS for simplicity.
Concurrency control is achieved using a single global blocking RWMutex lock.

NB:
// B-Tree implementations have many implementation specific details and optimisations before
// they're 'production' ready, notably they may use a free-list to hold cells in the leaf nodes,
// employ CoW semantics and support sophisticated concurrency mechanisms.

// FILE FORMAT
// todo!

visualisation: https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html

// learn more:
// etcd: https://pkg.go.dev/github.com/google/btree
// sqlite: https://sqlite.org/src/file/src/btree.c
// wiki: https://en.wikipedia.org/wiki/B%2B_tree

pseudocode: http://staff.ustc.edu.cn/~csli/graduate/algorithms/book6/chap19.htm
*/

package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	// 4KiB
	PAGE_SIZE = 4096
	// cap key sizes to fit into 8bytes for now
	MAX_NODE_KEY_SIZE = 8
	// 500 bytes per message/key's value else overflow
	MAX_NODE_VALUE_SIZE = 500
)

type nodeType int

// oh go, where art thy sum types? thine enums forsake me :(
const (
	ROOT_NODE nodeType = iota
	INTERNAL_NODE
	LEAF_NODE
)

// a contigous 4kiB chunck of memory
type page struct {
	id    uint64
	cells []cell
}

// cell's are either:
// a key cell - holds only seperator keys and pointers to pages between neighbours
// a key/value cell - holds keys and data records ie isKeyCell = false
type cell struct {
	pageId    int64
	isKeyCell bool
	keySize   uint64
	valueSize uint64
	// tbd: maybe simplify by using int
	keyBytes   []byte
	dataRecord []byte
}

type BPlusTree struct {
	root   *node
	degree int
	sync.RWMutex
}

type node struct {
	keys     []int
	pageId   int64
	parent   *node
	next     *node
	sibling  *node
	children []*node
	kind     nodeType
}

func NewBPlusTree(degree int) *BPlusTree {
	// init the datafile
	file, err := os.Create("db")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	return &BPlusTree{
		root:   nil,
		degree: degree,
	}
}

func (t *BPlusTree) Insert(key int, value []byte) error {
	t.Lock()
	defer t.Unlock()

	if t.root == nil {
		t.root = &node{
			kind:     ROOT_NODE,
			keys:     nil,
			children: nil,
			sibling:  nil,
			next:     nil,
			parent:   nil,
			pageId:   0,
		}

	}

	return t.root.insert(t, key, value, t.degree)
}

func (t *BPlusTree) Search(key int) ([]byte, error) {
	t.RLock()
	defer t.RUnlock()

	if t.root == nil {
		return []byte{}, errors.New("empty tree")
	}
	return t.root.search(t, key)
}

func (n *node) insert(t *BPlusTree, key int, value []byte, degree int) error {
	file, err := os.OpenFile("db", os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return err
	}

	// if we have to open and close a file handle on each call that's bad..
	// but on the other hand if we hold the handle resource forever..
	// if only there was something we could do.. a pool perhaps?
	// "real" persistent B+ trees would never use the open/read/write/seek syscalls anyway.
	defer file.Close()

	// todo(FIX ME): this mapping of seperator key -> split is broken
	switch n.kind {
	case ROOT_NODE:
		if len(n.keys) > degree {
			n.splitChild(t, len(n.keys)/2, t.degree)
		} else {
			offset, err := syncToOffset(file, value)
			if err != nil {
				return err
			}
			n.pageId = offset
		}
	case LEAF_NODE:
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}

		offset, err := syncToOffset(file, value)

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
		n.children[i].insert(t, key, value, degree)
	}

	if len(n.keys) > degree {
		n.splitChild(t, len(n.keys)/2, t.degree)
	}

	return nil
}

func (node *node) search(t *BPlusTree, key int) ([]byte, error) {
	file, err := os.OpenFile("db", os.O_RDWR, 0644)

	if err != nil {
		return nil, err
	}

	defer file.Close()
	reader := bufio.NewReader(file)

	if node.kind == LEAF_NODE {
		// Binary search for the key in the leaf node's keys
		low, high := 0, len(node.parent.keys)-1
		for low <= high {
			mid := low + (high-low)/2
			if node.keys[mid] == key {
				// Calculate the offset and seek to it
				offset := int64(binary.BigEndian.Uint64([]byte(strconv.Itoa(node.keys[mid]))))
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
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
				low = mid + 1
			} else {
				high = mid - 1
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

// splitChild splits the child n of the current n at the specified index.
func (n *node) splitChild(t *BPlusTree, index, degree int) {
	// Create a new n to hold the keys and children that will be moved
	newNode := &node{
		keys:     make([]int, len(n.keys)-index),
		children: make([]*node, len(n.children)-index),
		kind:     LEAF_NODE,
		next:     n.next,
	}

	// Move keys and children to the new n
	copy(newNode.keys, n.keys[index:])
	copy(newNode.children, n.children[index:])
	n.keys = n.keys[:index]
	n.children = n.children[:index]

	// If the n is a leaf n, set the next pointer of the current n to the new n
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
	if len(parent.keys) > degree {
		parent.splitChild(t, len(parent.keys)/2, degree)
	}
}

func syncToOffset(file *os.File, value []byte) (int64, error) {
	writer := bufio.NewWriter(file)
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

	offset, err := file.Seek(0, os.SEEK_CUR)

	if err != nil {
		return 0, err
	}

	return offset, nil
}
