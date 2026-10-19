// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/gcash/goleveldb/leveldb/journal"
	"github.com/gcash/goleveldb/leveldb/memdb"
	"github.com/gcash/goleveldb/leveldb/storage"
)

var (
	errHasFrozenMem = errors.New("has frozen mem")
)

type memDB struct {
	db *DB
	*memdb.DB
	ref int32
}

func (m *memDB) getref() int32 {
	return atomic.LoadInt32(&m.ref)
}

func (m *memDB) incref() {
	atomic.AddInt32(&m.ref, 1)
}

func (m *memDB) decref() {
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 {
		// Only put back memdb with std capacity.
		if m.Capacity() == m.db.s.o.GetWriteBuffer() {
			m.Reset()
			m.db.mpoolPut(m.DB)
		}
		m.db = nil
		m.DB = nil
	} else if ref < 0 {
		panic("negative memdb ref")
	}
}

// Get latest sequence number.
func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

// Atomically adds delta to seq.
func (db *DB) addSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

func (db *DB) setSeq(seq uint64) {
	atomic.StoreUint64(&db.seq, seq)
}

// writesStopped returns the error that stopped writes and compactions until
// the DB is reopened, or nil.
func (db *DB) writesStopped() error {
	db.writeErrMu.Lock()
	defer db.writeErrMu.Unlock()
	return db.writeErr
}

// stopWrites stops writes and compactions until the DB is reopened, as
// LevelDB does once what is on disk is unknown, and returns the error that
// stopped them: err, unless they had already stopped.
func (db *DB) stopWrites(err error) error {
	db.writeErrMu.Lock()
	defer db.writeErrMu.Unlock()
	if db.writeErr == nil {
		db.writeErr = err
		db.logf("db@write %v", err)
	}
	return db.writeErr
}

func (db *DB) sampleSeek(ikey internalKey) {
	v := db.s.version()
	if v.sampleSeek(ikey) {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}
	v.release()
}

func (db *DB) mpoolPut(mem *memdb.DB) {
	if !db.isClosed() {
		select {
		case db.memPool <- mem:
		default:
		}
	}
}

func (db *DB) mpoolGet(n int) *memDB {
	var mdb *memdb.DB
	select {
	case mdb = <-db.memPool:
	default:
	}
	if mdb == nil || mdb.Capacity() < n {
		mdb = memdb.New(db.s.icmp, maxInt(db.s.o.GetWriteBuffer(), n))
	}
	return &memDB{
		db: db,
		DB: mdb,
	}
}

func (db *DB) mpoolDrain() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-db.memPool:
			default:
			}
		case <-db.closeC:
			ticker.Stop()
			// Drop what is pooled, but leave the pool open: a memdb
			// compaction still finishing can put a memdb back after Close
			// has begun, and a send on a closed channel panics.
			select {
			case <-db.memPool:
			default:
			}
			return
		}
	}
}

// Create new memdb and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
func (db *DB) newMem(n int) (mem *memDB, err error) {
	fd := storage.FileDesc{Type: storage.TypeJournal, Num: db.s.allocFileNum()}
	w, err := db.s.stor.Create(fd)
	if err != nil {
		db.s.reuseFileNum(fd.Num)
		return
	}

	db.memMu.Lock()
	defer db.memMu.Unlock()

	if db.frozenMem != nil {
		// Leave neither the new journal nor its open file behind.
		_ = w.Close()
		if db.s.stor.Remove(fd) == nil {
			db.s.reuseFileNum(fd.Num)
		}
		return nil, errHasFrozenMem
	}

	if db.journal == nil {
		db.journal = journal.NewWriter(w)
	} else {
		// Switch to the new journal even if finishing the old one fails, as
		// LevelDB does. The old journal may then have lost its last writes,
		// so stop writes until the DB is reopened. The callers hold the write
		// lock.
		rerr := db.journal.Reset(w)
		if cerr := db.journalWriter.Close(); rerr == nil {
			rerr = cerr
		}
		if rerr != nil {
			err = db.stopWrites(&writesStoppedError{"journal rotation", rerr})
		}
		db.frozenJournalFd = db.journalFd
	}
	db.journalWriter = w
	db.journalFd = fd
	db.frozenMem = db.mem
	mem = db.mpoolGet(n)
	mem.incref() // for self
	db.mem = mem
	// The seq only incremented by the writer. And whoever called newMem
	// should hold write lock, so no need additional synchronization here.
	db.frozenSeq = db.seq
	if err != nil {
		return nil, err
	}
	mem.incref() // for caller
	return
}

// Get all memdbs.
func (db *DB) getMems() (e, f *memDB) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	if db.frozenMem != nil {
		db.frozenMem.incref()
	}
	return db.mem, db.frozenMem
}

// Get effective memdb.
func (db *DB) getEffectiveMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}

// Get frozen memdb.
func (db *DB) getFrozenMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.frozenMem != nil {
		db.frozenMem.incref()
	}
	return db.frozenMem
}

// hasFrozenMem returns whether a frozen memdb is waiting to be flushed.
func (db *DB) hasFrozenMem() bool {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	return db.frozenMem != nil
}

// Drop frozen memdb; assume that frozen memdb isn't nil.
func (db *DB) dropFrozenMem() {
	db.memMu.Lock()
	db.frozenJournalFd = storage.FileDesc{}
	db.frozenMem.decref()
	db.frozenMem = nil
	db.memMu.Unlock()

	// The flushed journal is now obsolete, with any older one an earlier
	// flush failed to remove.
	db.removeObsoleteJournals()
}

// removeObsoleteJournals removes every journal older than the oldest one in
// use. A journal left behind by a failed removal must not outlive the next
// try: Recover replays every journal present, so it would bring back what
// was deleted after its flush.
func (db *DB) removeObsoleteJournals() {
	db.memMu.RLock()
	inUse := db.journalFd.Num
	if !db.frozenJournalFd.Zero() {
		inUse = db.frozenJournalFd.Num
	}
	db.memMu.RUnlock()

	fds, err := db.s.stor.List(storage.TypeJournal)
	if err != nil {
		db.logf("journal@remove listing %q", err)
		return
	}
	for _, fd := range fds {
		if fd.Num >= inUse {
			continue
		}
		if err := db.s.stor.Remove(fd); err != nil {
			db.logf("journal@remove removing @%d %q", fd.Num, err)
		} else {
			db.logf("journal@remove removed @%d", fd.Num)
		}
	}
}

// Clear mems ptr; used by DB.Close().
func (db *DB) clearMems() {
	db.memMu.Lock()
	db.mem = nil
	db.frozenMem = nil
	db.memMu.Unlock()
}

// Set closed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.closed, 0, 1)
}

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Check read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return ErrClosed
	}
	return nil
}
