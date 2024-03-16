package main

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

import (
	"fmt"
)

func main() {
	tree := NewBPlusTree(4)

	for i := 1; i < 10; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		tree.Insert(key, value)
	}

	// TODO: this should not be nil
	fmt.Println(tree.root.next)
}
