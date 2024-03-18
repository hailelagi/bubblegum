package main

import (
	"os"
)

type DB struct {
	datafile *os.File
	store    *BPlusTree
}

// if we have to open and close a file handle on each call that's bad..
// but on the other hand if we hold the handle resource forever..
// we would want to bind the lifetime of the file descriptor to the DB process/struct.
// or use a memory pool if pages aren't structured in a single file see for e.g Postgres.
// "real" persistent B+ trees would use the open/read/write/seek syscalls more sophisticatedly.
// see also alernatively: https://www.sqlite.org/mmap.html
func InitDB(store *BPlusTree) (*DB, error) {
	// init the datafile
	file, err := os.Create("db")

	if err != nil {
		return nil, err
	}

	db := &DB{datafile: file, store: store}
	db.store.db = db

	return db, nil
}

func (db *DB) Insert(key int, value []byte) error {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Insert(key, value)
}

func (db *DB) Query(key int) ([]byte, error) {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Search(key)
}
func (db *DB) Delete(key int) error {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Delete(key)
}

func (db *DB) Close() {
	db.datafile.Close()
}
