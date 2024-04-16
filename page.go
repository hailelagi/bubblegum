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

	// 255 bytes max cell data size, else overflow
	OVERFLOW_PAGE_SIZE = 255
)

// 16 byte page header
type pageHeader struct {
	PageID     uint32 // 4 bytes
	Reserve    uint32 // 4 bytes
	FreeSlots  uint16 // 2 bytes
	PLower     uint16 // 2 bytes
	PHigh      uint16 // 2 bytes
	NumSlots   byte   // 1 byte (uint8)
	CellLayout byte   // 1 byte (uint8)

}

// 100 - 16 byte header = 4080 bytes
// Page is (de)serialised disk block similar to: https://doxygen.postgresql.org/bufpage_8h_source.html
// It is a contigous 4kiB chunk of memory maintained in-memory(on init) + a disk repr.
// It is both a logical and physical representation of data.
// logically a page is organised in 'slots':
// [[header] [pointers/offsets to cells] [[cell][cell][cell]]]
type Page struct {
	pageHeader

	cellPointers []int16

	/*
		cells        []cell
	*/
}

// cell's hold individual key/value records, either:
// a key cell - holds only seperator keys and pointers to pages between neighbours
// a key/value cell - holds keys and data records ie isKeyCell = false
type cell struct {
	cellId    int16
	keySize   uint64
	valueSize uint64
	keys      []byte
	data      []byte
}

/*
Database files often consist of multiple parts, with a lookup table aiding navigation
and pointing to the start offsets of these parts written either in the file header,
trailer, or in the separate file.

DBMS uses an indirection layer to map pageIDs to offsets.
page directory? - maps page ids to offsets
*/
func (p *Page) MapToOffset() (int64, error) {
	return 0, nil
}

// Allocate creates an in-memory buffer of 4KiB that eventually is persisted
func (p *Page) Allocate() error {
	// todo: lift the pageId autoincrement globally to the DB struct
	// todo: create the page directory mechanism
	// p.offsetEnd = p.id * PAGE_SIZE
	// p.offsetBegin = p.offsetEnd - PAGE_SIZE
	p.pageHeader.PageID = 1
	p.pageHeader.FreeSlots = 64
	p.pageHeader.NumSlots = 64
	p.CellLayout = 1
	p.pageHeader.PLower = 100
	p.pageHeader.PHigh = 4196
	p.pageHeader.Reserve = 0

	/*
			if err != nil {
			return errors.New("internal error: could not allocate a new page")
		}

	*/

	return nil
}

// Fetch: retrieve an existing page from the buffer pool or pull from disk
// and decode the contents back into a memory page
func FetchPage(pageId int, datafile *os.File) (Page, error) {
	var page Page

	datafile.Seek(100, io.SeekStart)
	err := binary.Read(datafile, binary.LittleEndian, &page.pageHeader)

	if err != nil {
		return Page{}, err
	}

	// todo: binsearch/decode cell

	return page, nil
}

// TODO(nice-to-have): checksum pages using md5
// Flush: flush dirty pages and encode mem layout into bytes and write out disk
func (p *Page) Flush(datafile *os.File) error {
	buf := new(bytes.Buffer)

	// Seek to the position of the pageHeader within the file
	_, err := datafile.Seek(int64(p.PLower), io.SeekStart)
	if err != nil {
		log.Fatalf("error seeking: %v", err)
	}

	err = binary.Write(buf, binary.LittleEndian, &p.pageHeader)
	if err != nil {
		log.Fatalf("buffer write failed: %v", err)
	}

	n, err := buf.WriteTo(datafile)
	if err != nil {
		log.Fatalf("db-EIO: %v", err)
	}

	err = datafile.Sync()
	if err != nil {
		log.Fatalf("unrecoverable error during fsync: %v", err)
	}

	log.Printf("written %v bytes to disk at pageID %v", n, p.PageID)
	return nil
}
