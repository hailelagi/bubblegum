package main

import (
	"testing"
)

func FuzzUpsertKeys(f *testing.F) {
	tree := NewBTree(3)

	for key := 1; key < 100_000; key++ {
		f.Add(key)
	}

	f.Fuzz(func(t *testing.T, key int) {
		_ = tree.Upsert(key, key)
		found := keyExists(tree, key)

		if !found {
			t.Errorf("not found %v", key)
		}
	})
}

func FuzzGetKeys(f *testing.F) {
	tree := NewBTree(3)

	for key := 1; key < 100_000; key++ {
		f.Add(key)
	}

	f.Fuzz(func(t *testing.T, key int) {
		var found bool
		_ = tree.Upsert(key, key)

		data, _, err := tree.Get(key)

		if err != nil {
			t.Errorf("could not search tree %v", err)
		}

		for _, d := range data {
			if d == key {
				found = true
			}
		}

		if !found {
			t.Errorf("did not find key Upserted")
		}
	})
}

func FuzzDeleteKeys(f *testing.F) {
	tree := NewBTree(3)

	for key := 1; key < 100_000; key++ {
		f.Add(key)
	}

	f.Fuzz(func(t *testing.T, key int) {
		_ = tree.Upsert(key, key)
		err := tree.Delete(key)

		if err != nil {
			t.Errorf("deletion errored %v", err)
		}

		v, _, _ := tree.root.search(key)

		for _, d := range v.data {
			if d == key {
				t.Errorf("found deleted key/value %v", v)
			}
		}

	})
}

func keyExists(t *BTree, key int) bool {
	n, _, _ := t.root.search(key)

	for _, v := range n.data {
		if v == key {
			return true
		}
	}

	return false
}
