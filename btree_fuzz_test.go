package main

import (
	"bytes"
	"fmt"
	"testing"
)

func FuzzInsert(f *testing.F) {
	tree := NewBTree(4)
	for key := 1; key < 4; key++ {
		value := []byte(fmt.Sprint("msg_\n", key))
		f.Add(key, value)
	}

	f.Fuzz(func(t *testing.T, key int, value []byte) {
		tree.Insert(key, value)
		result, _ := tree.Get(key)

		if !bytes.Equal(value, result) {
			panic("non matching insert and access result")
		}
	})
}
