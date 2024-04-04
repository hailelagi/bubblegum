package main

import (
	"os"
	"syscall"
)

type Store[Key comparable, Value byte] interface {
	Get(key Key) ([]Value, error)
	Insert(key Key, value []Value) error
	Scan() ([][]Value, error)
	Range(start, end Key) ([][]Value, error)
	Delete(key Key) error
}

// if we have to open and close a file handle on each call that's bad..
// but on the other hand if we hold the handle resource forever..
// we would want to bind the lifetime of the file descriptor to the DB process/struct.
// or use a memory pool if pages aren't structured in a single file see for e.g Postgres.
// "real" persistent B+ trees would use the open/read/write/seek syscalls more sophisticatedly.
// see also alernatively: https://www.sqlite.org/mmap.html
type DB[s Store[int, byte]] struct {
	datafile *os.File
	store    s
}

func InitDB[s BTree | LSMTree](store *s) (*DB[s], error) {
	// init the datafile
	file, err := syscall.Open("db", syscall.O_RDWR|syscall.O_DSYNC|syscall.O_TRUNC, 0)
	if err != nil {
		return nil, err
	}

	return &DB[s]{datafile: os.NewFile(uintptr(file), "db"), store: store}, nil
}

func (db *DB[Store]) Insert(key int, value []byte) error {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Insert(key, value)
}

func (db *DB[Store]) Get(key int) ([]byte, error) {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Get(key)
}
func (db *DB[store]) Delete(key int) error {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Delete(key)
}

func (db *DB[store]) Close() {
	db.datafile.Close()
}
