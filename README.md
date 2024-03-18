# btree

A 'simple' On-Disk B Plus Tree.
Persistence is achieved using a naive IO buffer managed by the OS and pages are flushed with `fsync`.
Concurrency control is achieved using a single global blocking RWMutex lock.

```bash
$ go get
$ go test .
```

run example:
```
go run .
```
