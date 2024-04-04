package main

import (
	"errors"
	"sync"
)

type LSMTree struct {
	memtable  []int
	sstable   [][]byte
	threshold int

	sync.RWMutex
}

func (t *LSMTree) Get(key int) ([]byte, error) {
	return []byte{}, nil

}

func (t *LSMTree) Insert(key int, value []byte) error {
	t.Lock()
	defer t.Unlock()

	return nil
}

func (t *LSMTree) Scan() ([][]byte, error) {
	return nil, errors.New("unimplemented")
}

func (t *LSMTree) Range(start, end int) ([][]byte, error) {
	return nil, nil
}

func (t *LSMTree) Delete(key int) error {
	t.Lock()
	defer t.Unlock()

	return nil
}
