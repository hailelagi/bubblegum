package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var key = 1
var value = []byte(fmt.Sprint("msg_", key))
var testValueSize = cap(value)

func TestInsertRoot(t *testing.T) {
	tree := NewBTree(2)
	db, _ := InitDB(tree)

	defer db.Close()

	tree.db = db

	errInsert := tree.Insert(key, value)
	tree.Insert(key, value)
	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	if errInsert != nil {
		t.Errorf("Error inserting key %d: %v", key, errInsert)
	}

	expectedBuf := make([]byte, testValueSize)
	file.Read(expectedBuf)

	// TODO: make this test less dumb
	assert.Equal(t, value, expectedBuf[:testValueSize-3])
}

/*

func TestInsertAnDSearchRoot(t *testing.T) {
	tree := NewBTree(4)
	db, _ := InitDB(tree)
	defer db.Close()

	err := tree.Insert(key, value)
	tree.Insert(key, value)

	// TODO: magic number of offset
	key := 5

	result, errSearch := tree.Get(5)

	if err != nil {
		t.Errorf("Error inserting key %d: %v", key, err)
	}

	if errSearch != nil {
		t.Errorf("Error searching key %d: %v", key, err)
	}

	_assert.Equal(t, value, result)
}

func TestInsertKeysBeforeSplit(t *testing.T) {
	tree := NewBTree(3)
	db, _ := InitDB(tree)
	defer db.Close()

	var expectedKeys []byte

	for i := 1; i < 3; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		expectedKeys = append(expectedKeys, value...)

		tree.Insert(key, value)
	}

	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	gotBuf := make([]byte, testValueSize+4)
	_, e := file.Read(gotBuf)

	if !bytes.Equal(expectedKeys, gotBuf) || e != nil {
		t.Errorf("Error key does not match result")
	}
}

func TestInsertKeysAfterSplit(t *testing.T) {
	tree := NewBTree(3)
	db, _ := InitDB(tree)
	defer db.Close()

	var expectedKeys []byte

	for i := 1; i < 9; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		expectedKeys = append(expectedKeys, value...)

		tree.Insert(key, value)
	}

	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	gotBuf := make([]byte, testValueSize*4)
	file.Read(gotBuf)

	// TODO: make this test less dumb
	_assert.Equal(t, expectedKeys[:testValueSize*4], gotBuf[:testValueSize*4])
}

/*
func TestInsertAndAccessBTree(t *testing.T) {
	tree := NewBTree(4)

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
