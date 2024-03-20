package main

import (
	"bytes"
	"encoding/binary"
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

/*
todo: track empty page size/occupancy
*/

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

func NewPage(datafile *os.File) (*Page, error) {
	page := Page{}
	err := page.Allocate(datafile)

	if err != nil {
		return nil, err
	}

	return &page, nil
}

/*
DBMS uses an indirection layer to map pageIDs to offsets.
page directory? - maps page ids to offsets
*/
func (p *Page) MapToOffset() (int64, error) {
	return 0, nil
}

func (p *Page) Allocate(datafile *os.File) error {
	// todo: lift the pageId autoincrement globally to the DB struct
	// todo: create the page directory mechanism
	p.offsetEnd = p.id * PAGE_SIZE
	p.offsetBegin = p.offsetEnd - PAGE_SIZE

	// todo: preallocate cells with make()

	/*
			if err != nil {
			return errors.New("internal error: could not allocate a new page")
		}

	*/

	return nil
}

// Fetch: retrieve an existing page from the buffer pool or pull from disk
// and decode the contents back into a memory page
func Fetch(pageId int) error {
	return nil
}

// Flush: flush dirty pages and encode into raw bytes to disk
func (p *Page) Flush(db *DB) error {
	// Encode the page into bytes
	buf := new(bytes.Buffer)

	_, err := db.datafile.Seek(p.offsetBegin, io.SeekStart)
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
