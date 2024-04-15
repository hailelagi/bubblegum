package main

/*
func TestFlushCleanPage(t *testing.T) {
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
