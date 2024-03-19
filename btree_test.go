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
	tree := NewBPlusTree(2)
	db, _ := InitDB(tree)

	defer db.Close()

	errInsert := tree.Insert(key, value)
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
func TestInsertKeysBeforeSplit(t *testing.T) {
	tree := NewBPlusTree(3)
	db, _ := InitDB(tree)
	defer db.Close()

	var expectedKeys []byte

	for i := 1; i < 4; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		expectedKeys = append(expectedKeys, value...)

		tree.Insert(key, value)
	}

	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	gotBuf := make([]byte, testValueSize*4)
	_, e := file.Read(gotBuf)

	// TODO: refactor this assertion to rely less on magic numbers
	if !bytes.Equal(expectedKeys, gotBuf[:testValueSize*4-14]) || e != nil {
		t.Errorf("Error key does not match result")
	}
}

func TestInsertKeysAfterSplit(t *testing.T) {
	tree := NewBPlusTree(3)
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
	assert.Equal(t, expectedKeys[:testValueSize*4], gotBuf[:testValueSize*4])
}

func TestInsertAnDSearchRoot(t *testing.T) {
	tree := NewBPlusTree(4)

	err := tree.Insert(key, value)
	result, errSearch := tree.Search(key)

	if err != nil {
		t.Errorf("Error inserting key %d: %v", key, err)
	}

	if errSearch != nil {
		t.Errorf("Error searching key %d: %v", key, err)
	}

	assert.Equal(t, value, result)
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
