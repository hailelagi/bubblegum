# bubblegum 🍬

An On-Disk B+ Tree storage engine built as a part of reading [Database Internals](https://www.databass.dev/).
 Bubblegum is a toy project and an excuse to dive into and learn ideas from [badger](https://github.com/dgraph-io/badger), [pebble](https://github.com/cockroachdb/pebble), [bolt/bbolt](https://github.com/etcd-io/bbolt) and [etcd](https://github.com/etcd-io/etcd).

## current goals
Persistence is achieved using a simple/naive buffer pool ontop of the `read`, `write`, `lseek` syscalls and pages are flushed with `fsync`.
Concurrency control is achieved using a single global blocking RWMutex lock(for now!).

## File Format
TODO:
| x | y | z |
| ... | ... |

```bash
$ go get
$ go test .
```

run example:
```
go run .
```
