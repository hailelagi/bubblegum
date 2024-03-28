package main

type LSM struct {
	memtable  []int
	sstable   [][]byte
	threshold int
}
