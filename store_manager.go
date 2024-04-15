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
