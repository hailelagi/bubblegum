package main

// Storage manager - responsible for maintaining datafiles.
// LIFO simple queue maybe
// simple statstistics maybe

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
