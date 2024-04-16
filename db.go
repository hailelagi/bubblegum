package main

import (
	"log"
	"os"
	"syscall"
)

// The storage engine high level api
type Store interface {
	Get(key int) ([]byte, error)
	Insert(key int, value []byte) error
	Scan() ([][]byte, error)
	Range(start, end int) ([][]byte, error)
	Delete(key int) error
}

// if we have to open and close a file handle on each call that's bad..
// but on the other hand if we hold the handle resource forever..
// we would want to bind the lifetime of the file descriptor to the DB process/struct.
// or use a memory pool if pages aren't structured in a single file see for e.g Postgres.
// "real" persistent B+ trees would use the open/read/write/seek syscalls more sophisticatedly.
// see also alernatively: https://www.sqlite.org/mmap.html
type DB struct {
	datafile     *os.File
	store        Store
	storeManager StoreManager
}

func InitDB(store Store, dbname string) (*DB, error) {
	// init the datafile
	init, err := os.Create(dbname)

	if err != nil {
		log.Fatal(err)
	}

	defer init.Close()

	file, err := syscall.Open(dbname, syscall.O_RDWR|syscall.O_DSYNC|syscall.O_TRUNC, 0)
	if err != nil {
		log.Fatal(err)
	}

	datafile := os.NewFile(uintptr(file), "db")
	manager := StoreManager{datafile: datafile}

	if dbname != "test_db" {
		manager.InitHeader()
	}

	return &DB{datafile: datafile, store: store, storeManager: manager}, nil
}

/*** Access Methods ***/

func (db *DB) Insert(key int, value []byte) error {
	return db.store.Insert(key, value)
}

func (db *DB) Get(key int) ([]byte, error) {
	s := db.store

	if s != nil {
		return db.store.Get(key)
	}

	return nil, nil
}

func (db *DB) Delete(key int) error {
	// todo stub out key for Interface{} or parameterise this
	// todo handle high level datatypes, int & str
	return db.store.Delete(key)
}

func (db *DB) Close() {
	db.datafile.Close()
}
