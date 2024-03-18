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
	"log"
)

func main() {
	db, err := InitDB(NewBPlusTree(4))

	if err != nil {
		log.Fatalf("could not init database cause: %v", err)
	}

	for i := 1; i < 10; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		db.Insert(key, value)
	}

	/*
		for i := 1; i < 10; i++ {
			res, _ := db.Query(i)
			value := []byte(fmt.Sprint("msg_", i, "\n"))
			Assert(bytes.Equal(res, value), "read your writes :) ")
		}
	*/

	/*
		for i := 1; i < 10; i++ {
			db.Delete(i)
			err := db.Delete(i)

			Assert(err != nil, "value must not be found after deletion")
		}
	*/

	// cleanup
	db.Close()
}

// why? see: https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md#safety
func Assert(cond bool, errMsg string, v ...any) {
	if !cond {
		panic(fmt.Sprintf("runtime invariant failure: "+errMsg, v...))
	}
}
