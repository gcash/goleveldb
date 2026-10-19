# goleveldb

[![Build Status](https://github.com/gcash/goleveldb/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/gcash/goleveldb/actions/workflows/ci.yml)

This is an implementation of the [LevelDB key/value database](https://github.com/google/leveldb) in the [Go programming language](https://go.dev).

It is a maintained fork of [syndtr/goleveldb](https://github.com/syndtr/goleveldb).

## Installation

    go get github.com/gcash/goleveldb/leveldb

## Switching from syndtr/goleveldb

To switch, change the import paths to `github.com/gcash/goleveldb`. A
`replace` directive does not work, since Go requires the replacement to
declare the module path it replaces.

Besides bug fixes, the behavior differs from syndtr/goleveldb in these ways:

- After a failed journal write or sync, a failure to finish the old journal
  when switching to a new one, or a failed MANIFEST commit, writes and
  compactions stop until the DB is reopened, and their errors match
  `leveldb.ErrWritesStopped`, as does the error `Close` then returns. Reads
  keep working. LevelDB stops the same way, except after a failed journal
  write.
- Journal recovery replays every batch, even those below the MANIFEST's
  sequence, and always verifies checksums, as LevelDB does.
  `opt.StrictJournalChecksum` no longer has any effect.
- MANIFEST recovery is always strict, as in LevelDB: a corrupted MANIFEST
  record fails the open instead of being skipped, and `opt.StrictManifest`
  no longer has any effect. A last record that a crash cut off, a commit
  that never finished, is still dropped. Use `Recover` to rebuild a DB whose
  MANIFEST is corrupted.
- Under `opt.StrictJournal`, a last journal record that a crash cut off is
  dropped, as in LevelDB, instead of failing the open. A strict
  `journal.Reader` drops it too, instead of halting.

## Requirements

- Go 1.21 or newer.

## Usage

Create or open a database:

```go
// The returned DB instance is safe for concurrent use. This means all DB
// methods may be called concurrently from multiple goroutines.
db, err := leveldb.OpenFile("path/to/db", nil)
...
defer db.Close()
...
```

Read or modify the database content:

```go
// Remember that the contents of the returned slice should not be modified.
data, err := db.Get([]byte("key"), nil)
...
err = db.Put([]byte("key"), []byte("value"), nil)
...
err = db.Delete([]byte("key"), nil)
...
```

Iterate over database content:

```go
iter := db.NewIterator(nil, nil)
for iter.Next() {
	// Remember that the contents of the returned slice should not be modified, and
	// only valid until the next call to Next.
	key := iter.Key()
	value := iter.Value()
	...
}
iter.Release()
err = iter.Error()
...
```

Seek-then-Iterate:

```go
iter := db.NewIterator(nil, nil)
for ok := iter.Seek(key); ok; ok = iter.Next() {
	// Use key/value.
	...
}
iter.Release()
err = iter.Error()
...
```

Iterate over subset of database content:

```go
iter := db.NewIterator(&util.Range{Start: []byte("foo"), Limit: []byte("xoo")}, nil)
for iter.Next() {
	// Use key/value.
	...
}
iter.Release()
err = iter.Error()
...
```

Iterate over subset of database content with a particular prefix:

```go
iter := db.NewIterator(util.BytesPrefix([]byte("foo-")), nil)
for iter.Next() {
	// Use key/value.
	...
}
iter.Release()
err = iter.Error()
...
```

Batch writes:

```go
batch := new(leveldb.Batch)
batch.Put([]byte("foo"), []byte("value"))
batch.Put([]byte("bar"), []byte("another value"))
batch.Delete([]byte("baz"))
err = db.Write(batch, nil)
...
```

Use bloom filter:

```go
o := &opt.Options{
	Filter: filter.NewBloomFilter(10),
}
db, err := leveldb.OpenFile("path/to/db", o)
...
defer db.Close()
...
```

## Documentation

Package documentation is on [pkg.go.dev](https://pkg.go.dev/github.com/gcash/goleveldb).
