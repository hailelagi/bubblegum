package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocandFlushRoot(t *testing.T) {
	tree := NewBTree(2)
	db, _ := InitDB(tree, "db")
	defer db.Close()

	page, err := db.storeManager.NewPage()

	if err != nil {
		t.Errorf("page allocation error: %v", err)
	}

	page.Flush(db.datafile)

	stat, err := db.datafile.Stat()

	if err != nil {
		t.Error()
	}

	assert.Equal(t, 117, int(stat.Size()))

	p, err := FetchPage(1, db.datafile)

	if err != nil {
		t.Error()
	}

	assert.Equal(t, 1, int(p.PageID))
	assert.Equal(t, 64, int(p.NumSlots))
}

/*
func TestFlushCleanPage(t *testing.T) {
	tree := NewBTree(2)
	db, _ := InitDB(tree)
	defer db.Close()

	page, err := NewPage(db.datafile)

	if err != nil {
		t.Errorf("page allocation error: %v", err)
	}

	page.Flush(db.datafile)

	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	expectedBuf := make([]byte, 100)
	file.Read(expectedBuf)

	assert.Equal(t, value, expectedBuf)
}
*/

/*
func TestFlushDirtyPage(t *testing.T) {
	tree := NewBTree(2)
	db, _ := InitDB(tree)
	defer db.Close()

	page, err := NewPage()

	if err != nil {
		t.Errorf("page allocation error: %v", err)
	}

	page.Flush(db)

	file, _ := os.OpenFile("db", os.O_RDONLY, 0644)
	defer file.Close()

	expectedBuf := make([]byte, testValueSize)
	file.Read(expectedBuf)

	_assert.Equal(t, value, expectedBuf)
}

*/
