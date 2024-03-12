package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

var key = 1
var value = []byte(fmt.Sprint("msg_", key))
var testValueSize = cap(value)

func TestInsert(t *testing.T) {
	tree := NewBPlusTree(4)

	errInsert := tree.Insert(key, value)
	file, errOpen := os.OpenFile("db", os.O_RDONLY, 0644)

	if errInsert != nil {
		t.Errorf("Error inserting key %d: %v", key, errInsert)
	}

	if errOpen != nil {
		t.Errorf(errOpen.Error())
	}

	defer file.Close()

	expectedBuf := make([]byte, testValueSize)
	_, errRead := file.Read(expectedBuf)

	// TODO: refactor this assertion to rely less on magic numbers
	// ignore the zero byte new line delimiter
	if !bytes.Equal(value, expectedBuf[:testValueSize-3]) || errRead != nil {
		t.Errorf("did not write the correct message key")
	}

}

func TestInsertKeysBeforeSplit(t *testing.T) {
	tree := NewBPlusTree(4)
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
	var expectedKeys []byte

	for i := 1; i < 7; i++ {
		key := i
		value := []byte(fmt.Sprint("msg_", i, "\n"))
		expectedKeys = append(expectedKeys, value...)

		tree.Insert(key, value)
	}

	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	gotBuf := make([]byte, testValueSize*4)
	_, e := file.Read(gotBuf)

	if !bytes.Equal(expectedKeys, gotBuf[:testValueSize*4-14]) || e != nil {
		t.Errorf("Error key does not match result")
	}
}

/*
func TestInsertAnDSearchBTree(t *testing.T) {
	tree := NewBPlusTree(4)

	err := tree.Insert(key, value)
	result, errSearch := tree.Search(key)

	if err != nil {
		t.Errorf("Error inserting key %d: %v", key, err)
	}

	if errSearch != nil {
		t.Errorf("Error searching key %d: %v", key, err)
	}

	if !bytes.Equal(value, result) {
		t.Errorf("Error result mismatch")
	}
}

*/

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
