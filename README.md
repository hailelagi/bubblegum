# btree

An On-Disk B+ Tree storage engine built as a part of reading [Database Internals](https://www.databass.dev/).

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
