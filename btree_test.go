package main

import (
	"testing"
)

func BenchmarkInsertBTree(b *testing.B) {
	tree := NewBPlusTree(3)

	for i := 0; i < 100_000; i++ {
		key := i
		value := i * 10
		err := tree.Insert(key, value)
		if err != nil {
			b.Errorf("Error inserting key %d: %v", key, err)
		}
	}
}

func BenchmarkAccessBTree(b *testing.B) {
	tree := NewBPlusTree(3)

	for i := 0; i < 100_000; i++ {
		key := i
		value := i * 10
		err := tree.Insert(key, value)
		if err != nil {
			b.Errorf("Error inserting key %d: %v", key, err)
		}
	}

	for i := 0; i < 100_000; i++ {
		key := i
		value := i * 10
		err := tree.Insert(key, value)
		result, errGet := tree.Search(key)

		if err != nil {
			b.Errorf("Error inserting key %d: %v", key, err)
		}

		if err != errGet {
			b.Errorf("Error inserting key %d: %v", key, err)
		}

		if key != result {
			b.Errorf("Error key does not match result")
		}
	}

}
