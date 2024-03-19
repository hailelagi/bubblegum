package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
)

const (
	// 4KiB
	PAGE_SIZE = 4096
	// cap key sizes to fit into 8bytes for now
	MAX_NODE_KEY_SIZE = 8
	// 500 bytes per message/key's value else overflow
	MAX_NODE_VALUE_SIZE = 500
)

// a contigous 4kiB chunk of memory maintained in-memory ie the "buffer pool"
type Page struct {
	// the page ID
	id int64
	// the physical offset mapping to the begining
	// and end of an allocated virtual memory segment block on the datafile "db"
	offsetBegin int64
	offsetEnd   int64
	cells       []cell
}

// cell's hold individual key/value records
// cell's are either:
// a key cell - holds only seperator keys and pointers to pages between neighbours
// a key/value cell - holds keys and data records ie isKeyCell = false
type cell struct {
	pageId    int64
	isKeyCell bool
	keySize   uint64
	valueSize uint64
	// tbd: maybe simplify by using int
	keyBytes   []byte
	dataRecord []byte
}

func NewPage() (*Page, error) {
	page := Page{}
	_, err := page.Allocate()

	if err != nil {
		return nil, err
	}

	return &page, nil
}

// NB: datafile must exist before trying to allocate a page.
func (p *Page) Allocate() (uint32, error) {
	file, err := os.OpenFile("db", os.O_RDWR|os.O_APPEND, 0644)
	offset, errSeek := file.Seek(0, io.SeekEnd)

	if err != nil || errSeek != nil {
		return 0, errors.New("internal error: could not allocate a new page")
	}
	defer file.Close()

	// NB: this does not actually write [yet]
	// todo: lift the pageId autoincrement globally to the DB struct
	startOffset := uint32(offset)
	endOffset := startOffset + PAGE_SIZE

	return endOffset, nil
}

func (p *Page) Flush(db *DB) error {
	// Encode the page into bytes
	buf := new(bytes.Buffer)

	// todo(FIXME): come back when init'd fixed size cells
	// p = NewPage with alloc'd cells
	if err := binary.Write(buf, binary.LittleEndian, p); err != nil {
		return err
	}
	pageBytes := buf.Bytes()

	// Write the page bytes to the file at the calculated offsets
	// TODO(nice-to-have): checksum pages using md5
	n, err := db.datafile.WriteAt(pageBytes, int64(p.offsetBegin))
	syncErr := db.datafile.Sync()

	if err != nil {
		return err
	}

	if syncErr != nil {
		return err
	}

	log.Printf("writen %v bytes to disk at pageID %v", n, p.id)
	return nil
}
