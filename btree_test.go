package main

import (
	"fmt"
	"testing"
)

/*
func TestInsertBTree(t *testing.T) {
	tree := NewBPlusTree(4)

	for i := 0; i < 2; i++ {
		key := i
		value := i * 10
		err := tree.Insert(key, value)
		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}
	}
}

*/

func TestAccessBTree(t *testing.T) {
	tree := NewBPlusTree(4)

	for i := 0; i < 2; i++ {
		key := i
		value := []byte(fmt.Sprintf("msg_", i))
		err := tree.Insert(key, value)
		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}
	}

	for i := 1; i < 3; i++ {
		key := i
		value := []byte(fmt.Sprintf("msg_", i))
		err := tree.Insert(key, value)
		result, errGet := tree.Search(key)

		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}

		if err != errGet {
			t.Errorf("Error inserting key %d: %v", key, err)
		}

		if value != result {
			t.Errorf("Error key does not match result")
		}
	}

}
