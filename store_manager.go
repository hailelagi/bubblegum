package main

import (
	"io"
	"log"
	"os"
)

// Storage manager - responsible for maintaining datafiles.
// LIFO simple queue maybe
// simple statstistics maybe

type StoreManager struct {
	datafile *os.File
}

func (s *StoreManager) InitHeader() {
	// reserve first 100 bytes, later stuff meta info here
	header := make([]byte, 100)
	s.datafile.Seek(0, io.SeekStart)

	_, err := s.datafile.Write(header)

	if err != nil {
		log.Fatalf("initial db setup failure %v", err)
	}
}

func (*StoreManager) NewPage() (*Page, error) {
	page := Page{}
	err := page.Allocate()

	if err != nil {
		return nil, err
	}

	return &page, nil
}

// must implement mapper
/*
Database files often consist of multiple parts, with a lookup table aiding navigation
and pointing to the start offsets of these parts written either in the file header,
trailer, or in the separate file.

DBMS uses an indirection layer to map pageIDs to offsets.
page directory? - maps page ids to offsets
*/

/*
todo: track empty page size/occupancy
*/

// 8 byte header + trailer
type reserve [8]uint8

func reserveHeadAndTrail() {
	// todo reserve 16bytes at the start and end of the data file
	// on init
}
