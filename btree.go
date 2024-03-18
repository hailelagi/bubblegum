package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"sync"
)

type nodeType int

// oh go, where art thy sum types? thine enums forsake me :(
const (
	ROOT_NODE nodeType = iota
	INTERNAL_NODE
	LEAF_NODE
)

type BPlusTree struct {
	root   *node
	degree int
	sync.RWMutex
}

type node struct {
	kind         nodeType
	pageId       int64
	keys         []int
	next         *node
	parent       *node
	leftSibling  *node
	rightSibling *node
	children     []*node
}

func NewBPlusTree(degree int) *BPlusTree {
	// init the datafile
	file, err := os.Create("db")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	// invariant one: 2 <= no of children < 2 * branching factor
	// number of keys = no. of children/degree - 1
	// branching factor - 1 < num keys < 2 * branching factor - 1
	Assert(degree >= 2, "the minimum degree of a B+ tree must be greater than 2")

	// todo: preallocated 4kiB for a node
	return &BPlusTree{
		root: &node{
			kind: ROOT_NODE,
			// todo: use MAX_KEY and MAX CHILDREN
			keys:         make([]int, 0),
			children:     make([]*node, 0),
			leftSibling:  nil,
			rightSibling: nil,
			next:         nil,
			parent:       nil,
			pageId:       0,
		},
		degree: degree,
	}
}

func (t *BPlusTree) Insert(key int, value []byte) error {
	t.Lock()
	defer t.Unlock()

	// todo handle casting into/from datatypes
	return t.root.insert(t, key, value, t.degree)
}

func (t *BPlusTree) Search(key int) ([]byte, error) {
	t.RLock()
	defer t.RUnlock()

	// todo handle casting into/from datatypes
	return t.root.search(t, key)
}

func (t *BPlusTree) Delete(key int) error {
	t.Lock()
	defer t.Unlock()

	return t.root.delete(t, key, t.degree)
}

func (n *node) insert(t *BPlusTree, key int, value []byte, degree int) error {
	file, err := os.OpenFile("db", os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return err
	}

	// if we have to open and close a file handle on each call that's bad..
	// but on the other hand if we hold the handle resource forever..
	// we would want to bind the lifetime of the file descriptor to the DB process/struct.
	// or use a memory pool if pages aren't structured in a single file see for e.g Postgres.
	// "real" persistent B+ trees would use the open/read/write/seek syscalls differently.
	// see: https://www.sqlite.org/mmap.html

	defer file.Close()

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

			// TODO: this key thing
			// do you map offsets to the id directly?
			n.keys = append(n.keys, int(offset))
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
		slices.Sort(n.keys)
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

	fmt.Println(node.kind)

	defer file.Close()
	reader := bufio.NewReader(file)

	fmt.Println(node.keys)

	if node.kind == ROOT_NODE {
		for _, k := range node.keys {
			if k == key {
				fmt.Println("Iam not crazy")
				offset := int64(binary.BigEndian.Uint64([]byte(strconv.Itoa(k))))
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
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
	if node.kind == LEAF_NODE {
		low, high := 0, len(node.keys)-1

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

	/*
		// If the node is not a leaf node, recursively search in the appropriate child
		i := 0
		for i < len(node.keys) && key >= node.keys[i] {
			i++
		}

		return node.children[i].search(t, key) // Recursively search in child node
	*/

	return nil, nil
}

func (node *node) delete(t *BPlusTree, key, degree int) error {
	// find node using key
	// pass in node, to node stealSibling
	// assume node is root at first
	// this an optimisation maybe do later
	ok, err := node.stealSibling(t, degree)

	if err != nil {
		return err
	}

	if !ok {
		err := node.mergeChildren(t, degree)

		if err != nil {
			return err
		}
	}

	return nil
}

func (node *node) stealSibling(t *BPlusTree, degree int) (bool, error) {
	return false, nil
}

func (node *node) mergeChildren(t *BPlusTree, degree int) error {
	return nil
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

	offset, err := file.Seek(0, io.SeekCurrent)

	if err != nil {
		return 0, err
	}

	return offset, nil
}
