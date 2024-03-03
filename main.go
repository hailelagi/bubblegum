package main

import (
	"bytes"
	"fmt"
)

func main() {
	tree := NewBPlusTree(4)

	for i := 1; i < 4; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_\n", i))
		tree.Insert(key, value)
		result, _ := tree.Search(key)

		if !bytes.Equal(value, result) {
			panic("non matching insert and access result")
		}
	}
}
