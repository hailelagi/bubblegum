# [bubblegum](https://adventuretime.fandom.com/wiki/Princess_Bubblegum) 🍬

An On-Disk B+ Tree storage engine built as a part of reading [Database Internals](https://www.databass.dev/).
 Bubblegum is a toy project and an excuse to dive into and learn ideas from [badger](https://github.com/dgraph-io/badger), [pebble](https://github.com/cockroachdb/pebble), [bolt/bbolt](https://github.com/etcd-io/bbolt) and [etcd](https://github.com/etcd-io/etcd).

## DataFile Format/Bit Representation

```
| header |   Page(s)    | trailer |
| ...    | .. | .. | .. |  ...    |
```

Logically Pages/Slotted Pages:

header:
```
| header(fixed) |
|     ...       |
```

page:
```
| page  |
|header(field names)| (cell pointers) | (reserved) | cell| ... |
```

```bash
$ go get
$ go test .
```

run example:
```
go run .
```
