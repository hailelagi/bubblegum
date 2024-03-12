package main

import (
	"bytes"
	"fmt"
	"testing"
)

func TestInsertBTree(t *testing.T) {
	tree := NewBPlusTree(4)

	// TODO(important!) FIXME panics during promotion/resize
	for i := 0; i < 4; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i))
		err := tree.Insert(key, value)
		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}
	}
}

func TestInsertnSearchBTree(t *testing.T) {
	tree := NewBPlusTree(4)

	for i := 0; i < 4; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i))

		err := tree.Insert(key, value)
		result, errSearch := tree.Search(key)

		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}

		if errSearch != nil {
			t.Errorf("Error searching key %d: %v", key, err)
		}

		if !bytes.Equal(value, result) {
			t.Errorf("Error result mismatch")
		}
	}
}

/*
func TestInsertAndAccessBTree(t *testing.T) {
	tree := NewBPlusTree(4)

	for i := 0; i < 3; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_\n", i))
		err := tree.Insert(key, value)
		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}
	}

	for i := 1; i < 3; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_\n", i))
		err := tree.Insert(key, value)
		result, errGet := tree.Search(key)

		if err != nil {
			t.Errorf("Error inserting key %d: %v", key, err)
		}

		if err != errGet {
			t.Errorf("Error inserting key %d: %v", key, err)
		}

		if !bytes.Equal(value, result) {
			t.Errorf("Error key does not match result")
		}
	}

}

*/
