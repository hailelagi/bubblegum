package main

import (
	"log"
	"math/rand"
	"slices"
	"testing"
)

func TestBTreeSingleSplit(t *testing.T) {
	tree := NewBTree(3)
	elements := []int{5, 2, 1, 4}

	for _, e := range elements {
		tree.Upsert(e, e)
	}

	if slices.Compare(tree.root.keys, []int{2, 4}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].data, []int{1}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].data, []int{2}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[2].data, []int{4, 5}) != 0 {
		t.Fail()
	}
}

func TestBTreeSingleSplitDegreeFive(t *testing.T) {
	tree := NewBTree(5)
	elements := []int{5, 2, 1, 4, 8, 9, 7}

	for _, e := range elements {
		tree.Upsert(e, e)
	}

	if slices.Compare(tree.root.keys, []int{4, 7}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].data, []int{1, 2}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].data, []int{4, 5}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[2].data, []int{7, 8, 9}) != 0 {
		t.Fail()
	}
}

func TestBTreeMultiSplit(t *testing.T) {
	tree := NewBTree(3)
	elements := []int{5, 2, 1, 4, 6, 7, 8, 3}

	for _, e := range elements {
		tree.Upsert(e, e)
	}

	if slices.Compare(tree.root.keys, []int{4, 6}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].keys, []int{2}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].children[1].data, []int{2, 3}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].keys, []int{5}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].children[1].data, []int{5}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[2].keys, []int{7}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[2].children[1].data, []int{7, 8}) != 0 {
		t.Fail()
	}

}

func TestBTreeMultiDelete(t *testing.T) {
	tree := NewBTree(3)
	elements := []int{5, 2, 1, 4, 6, 7, 8, 3}

	for _, e := range elements {
		tree.Upsert(e, e)
	}

	// deletion works slightly differently from how one
	// would expect a b-tree to merge.
	// it prefers the leftmost neighbour and doesn't steal.

	tree.Delete(5)

	if slices.Compare(tree.root.keys, []int{4, 6}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].keys, []int{2}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].children[0].data, []int{1}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].children[1].data, []int{2, 3}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[0].children[2].data, []int{4}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].keys, []int{7}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].children[0].data, []int{6}) != 0 {
		t.Fail()
	}

	if slices.Compare(tree.root.children[1].children[1].data, []int{7, 8}) != 0 {
		t.Fail()
	}
}

func BenchmarkBTree(b *testing.B) {
	tree := NewBTree(3)

	for i := 0; i <= 100_000; i++ {
		key := i
		// value := i * 10
		err := tree.Upsert(key, key)
		if err != nil {
			b.Errorf("Error Upserting key %d: %v", key, err)
		}
	}

	b.ResetTimer()

	b.Run("write", func(pb *testing.B) {
		for i := 10_000; i < pb.N; i++ {
			// value := i * 10
			key := rand.Intn(100_000)
			err := tree.Upsert(key, key)

			if err != nil {
				b.Errorf("Error Upserting key %d: %v", i, err)
			}
		}
	})

	log.Printf("current node count: %v", tree.nodeCount)

	b.Run("access", func(pb *testing.B) {
		for i := 0; i < pb.N; i++ {
			key := rand.Intn(100_000)

			n, idx, err := tree.Get(key)
			if err != nil {
				b.Errorf("Error searching key %d: %v node: %v at: %v", key, err, n, idx)
			}
		}
	})

	log.Printf("current node count: %v", tree.nodeCount)

	b.Run("read/write", func(pb *testing.B) {
		for i := 0; i <= pb.N; i++ {
			key := rand.Intn(100_000)
			err := tree.Upsert(key, key)
			n, idx, searchErr := tree.Get(key)

			if searchErr != nil {
				b.Errorf("Error searching key %d: %v node: %v at: %v", i, err, n, idx)
			}

			if err != nil {
				b.Logf("warning Upserting %d: %v", i, err)
			}
		}
	})

	log.Printf("current node count: %v", tree.nodeCount)
}

func BenchmarkBTreeConcurrentAccess(b *testing.B) {
	tree := NewBTree(3)

	for i := 0; i <= 100_000; i++ {
		key := i
		// value := i * 10
		err := tree.Upsert(key, key)
		if err != nil {
			b.Errorf("Error Upserting key %d: %v", key, err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := rand.Intn(100_000)
			n, idx, err := tree.Get(key)

			if err != nil {
				b.Errorf("Error searching key %d: %v node: %v at: %v", key, err, n, idx)
			}
		}
	})
}

func BenchmarkBTreeConcurrentWriter(b *testing.B) {
	tree := NewBTree(3)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := rand.Intn(100_000)
			err := tree.Upsert(key, key)
			if err != nil {
				b.Errorf("Error Upserting key %d: %v", key, err)
			}
		}
	})
}

func BenchmarkBTreeIndexSampleRead(b *testing.B) {
	tree := NewBTree(500)

	for i := 0; i <= 1_000_000; i++ {
		key := i
		// value := i * 10
		err := tree.Upsert(key, key)
		if err != nil {
			b.Errorf("Error Upserting key %d: %v", key, err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := rand.Intn(1_000_000)
			n, idx, err := tree.Get(key)

			if err != nil {
				b.Errorf("Error searching key %d: %v node: %v at: %v", key, err, n, idx)
			}
		}
	})
}

func BenchmarkBTreeIndexSampleWrite(b *testing.B) {
	tree := NewBTree(500)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := rand.Intn(1_000_000)
			err := tree.Upsert(key, key)
			if err != nil {
				b.Errorf("Error Upserting key %d: %v", key, err)
			}
		}
	})
}
