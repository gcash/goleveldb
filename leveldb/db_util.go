// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/gcash/goleveldb/leveldb/errors"
	"github.com/gcash/goleveldb/leveldb/iterator"
	"github.com/gcash/goleveldb/leveldb/opt"
	"github.com/gcash/goleveldb/leveldb/storage"
	"github.com/gcash/goleveldb/leveldb/util"
)

// Reader is the interface that wraps basic Get and NewIterator methods.
// This interface implemented by both DB and Snapshot.
type Reader interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// Sizes is list of size.
type Sizes []int64

// Sum returns sum of the sizes.
func (sizes Sizes) Sum() int64 {
	var sum int64
	for _, size := range sizes {
		sum += size
	}
	return sum
}

// Logging.
func (db *DB) log(v ...interface{})                 { db.s.log(v...) }
func (db *DB) logf(format string, v ...interface{}) { db.s.logf(format, v...) }

// Check and clean files.
func (db *DB) checkAndCleanFiles() error {
	v := db.s.version()
	defer v.release()

	tmap := make(map[int64]bool)
	for _, tables := range v.levels {
		for _, t := range tables {
			tmap[t.fd.Num] = false
		}
	}

	fds, err := db.s.stor.List(storage.TypeAll)
	if err != nil {
		return err
	}

	var nt int
	var rem []storage.FileDesc
	for _, fd := range fds {
		keep := true
		switch fd.Type {
		case storage.TypeManifest:
			keep = fd.Num >= db.s.manifestFd.Num
		case storage.TypeJournal:
			if !db.frozenJournalFd.Zero() {
				keep = fd.Num >= db.frozenJournalFd.Num
			} else {
				keep = fd.Num >= db.journalFd.Num
			}
		case storage.TypeTable:
			_, keep = tmap[fd.Num]
			if keep {
				tmap[fd.Num] = true
				nt++
			}
		case storage.TypeTemp:
			// Only Recover writes temp files, and it is done with them
			// before the open gets here.
			keep = false
		}

		if !keep {
			rem = append(rem, fd)
		}
	}

	if nt != len(tmap) {
		var mfds []storage.FileDesc
		for num, present := range tmap {
			if !present {
				mfds = append(mfds, storage.FileDesc{Type: storage.TypeTable, Num: num})
				db.logf("db@janitor table missing @%d", num)
			}
		}
		return errors.NewErrCorrupted(storage.FileDesc{}, &errors.ErrMissingFiles{Fds: mfds})
	}

	// The files are obsolete, so a failed removal is only logged, as LevelDB
	// does. The next open retries it, and so do Close and, for journals,
	// every flush.
	db.logf("db@janitor F·%d G·%d", len(fds), len(rem))
	for _, fd := range rem {
		db.logf("db@janitor removing %s-%d", fd.Type, fd.Num)
		if err := db.s.stor.Remove(fd); err != nil {
			db.logf("db@janitor removing %s-%d %q", fd.Type, fd.Num, err)
		}
	}
	return nil
}

// liveTables returns the file numbers of the tables the current version lists.
func (db *DB) liveTables() map[int64]bool {
	v := db.s.version()
	defer v.release()
	live := make(map[int64]bool)
	for _, tables := range v.levels {
		for _, t := range tables {
			live[t.fd.Num] = true
		}
	}
	return live
}

// removeObsoleteTables removes every table file not in live, as the janitor
// does at open.
func (db *DB) removeObsoleteTables(live map[int64]bool) {
	fds, err := db.s.stor.List(storage.TypeTable)
	if err != nil {
		db.logf("db@close listing tables %q", err)
		return
	}
	for _, fd := range fds {
		if live[fd.Num] {
			continue
		}
		if err := db.s.stor.Remove(fd); err != nil {
			db.logf("db@close removing @%d %q", fd.Num, err)
		} else {
			db.logf("db@close removed @%d", fd.Num)
		}
	}
}
