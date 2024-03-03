/*
Simple Persistent B Plus Tree.
n keys are assumed to be signed integers and values a slice of bytes.
Persistence is achieved using a naive bufio.Writer interface for simplicity.
Concurrency control is achieved using a simple blocking RWMutex lock.

// B-Tree implementations have many implementation specific details and optimisations before
// they're 'production' ready, notably they may use a free-list to hold cells in the n,
// and support concurrency.
 (not implemented or discussed [yet], as // it's better explored as part of chapter 4)
// see also: CoW semantics

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
	"errors"
	"sync"
)

type BPlusTree struct {
	root   *node
	degree int
	sync.RWMutex
}

type node struct {
	keys     []int
	_data    *bufio.ReadWriter
	parent   *node
	next     *node
	children []*node
	isLeaf   bool
}

func NewBPlusTree(degree int) *BPlusTree {
	return &BPlusTree{
		root:   nil,
		degree: degree,
	}
}

func (t *BPlusTree) Insert(key int, value int) error {
	t.Lock()
	defer t.Unlock()

	if t.root == nil {
		t.root = &node{
			keys:     []int{key},
			children: []*node{},
			isLeaf:   true,
			next:     nil,
		}
	} else {
		t.root.insert(t, key, t.degree)
	}

	return nil
}

func (t *BPlusTree) Search(key int) (int, error) {
	t.RLock()
	defer t.RUnlock()

	if t.root == nil {
		return -1, errors.New("empty tree")
	}
	return t.root.search(key)
}

// insert inserts a key into the n.
func (n *node) insert(t *BPlusTree, key int, degree int) {
	if n.isLeaf {
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}
		n.keys = append(n.keys, 0)
		copy(n.keys[i+1:], n.keys[i:])
		n.keys[i] = key
	} else {
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i++
		}
		n.children[i].insert(t, key, degree)
	}
	if len(n.keys) > degree {
		n.splitChild(t, len(n.keys)/2, t.degree)
	}
}

// binary search.
func (node *node) search(key int) (int, error) {
	if node.isLeaf {
		low, high := 0, len(node.keys)-1
		for low <= high {
			mid := low + (high-low)/2
			if node.keys[mid] == key {
				return mid, nil
			} else if node.keys[mid] < key {
				low = mid + 1
			} else {
				high = mid - 1
			}
		}
		return -1, errors.New("key not found")
	}

	// If the node is not a leaf node, recursively search in the appropriate child
	i := 0
	for i < len(node.keys) && key >= node.keys[i] {
		i++
	}
	return node.children[i].search(key)
}

// splitChild splits the child n of the current n at the specified index.
func (n *node) splitChild(t *BPlusTree, index, degree int) {
	// Create a new n to hold the keys and children that will be moved
	newNode := &node{
		keys:     make([]int, len(n.keys)-index),
		children: make([]*node, len(n.children)-index),
		isLeaf:   n.isLeaf,
		next:     n.next,
	}

	// Move keys and children to the new n
	copy(newNode.keys, n.keys[index:])
	copy(newNode.children, n.children[index:])
	n.keys = n.keys[:index]
	n.children = n.children[:index]

	// If the n is a leaf n, set the next pointer of the current n to the new n
	if n.isLeaf {
		n.next = newNode
	}

	// If the current n is the root, create a new root and add the median key
	if n == t.root {
		newRoot := &node{
			keys:     []int{newNode.keys[0]},
			children: []*node{n, newNode},
			isLeaf:   false,
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
