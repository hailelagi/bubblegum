# btree

A 'simple' On-Disk B Plus Tree.
Persistence is achieved using a naive IO buffer managed by the OS for simplicity.
Concurrency control is achieved using a single global blocking RWMutex lock.

## file format
Pages are mapped to nodes and allocated in 4KiB chunks which are new line delimited.
