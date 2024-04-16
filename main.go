package main

/*
A toy Persistent/On-Disk B Plus Tree.

NB:
// B-Tree implementations have many implementation specific details and optimisations before
// they're 'production' ready, notably they may use a free-list to hold cells in the leaf nodes(in-memory),
// employ CoW semantics and support sophisticated concurrency mechanisms(MVCC).

visualisation: https://www.cs.usfca.edu/~galles/visualization/BTree.html

// learn more:
// etcd: https://pkg.go.dev/github.com/google/btree
// sqlite: https://sqlite.org/src/file/src/btree.c
// wiki: https://en.wikipedia.org/wiki/B%2B_tree

pseudocode: http://staff.ustc.edu.cn/~csli/graduate/algorithms/book6/chap19.htm
*/

import (
	"bytes"
	"fmt"
	"log"
)

func main() {
	tree := NewBTree(100)
	db, err := InitDB(tree, "db")

	if err != nil {
		log.Fatalf("could not init database cause: %v", err)
	}

	for i := 1; i < 10_000; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		db.Insert(key, value)
	}

	for i := 1; i < 10_000; i++ {
		res, _ := db.Get(i)
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		_assert(bytes.Equal(res, value), "read your writes :) ")
	}

	for i := 1; i < 10_000; i++ {
		db.Delete(i)
		err := db.Delete(i)

		_assert(err != nil, "value must not be found after deletion")
	}

	// cleanup
	db.Close()
}

// why? see: https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md#safety
func _assert(cond bool, errMsg string, v ...any) {
	if !cond {
		panic(fmt.Sprintf("runtime invariant failure: "+errMsg, v...))
	}
}
