// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gcash/goleveldb/leveldb/errors"
	"github.com/gcash/goleveldb/leveldb/journal"
	"github.com/gcash/goleveldb/leveldb/opt"
	"github.com/gcash/goleveldb/leveldb/storage"
)

// ErrManifestCorrupted records manifest corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrManifestCorrupted struct {
	Field  string
	Reason string
}

func (e *ErrManifestCorrupted) Error() string {
	return fmt.Sprintf("leveldb: manifest corrupted (field '%s'): %s", e.Field, e.Reason)
}

func newErrManifestCorrupted(fd storage.FileDesc, field, reason string) error {
	return errors.NewErrCorrupted(fd, &ErrManifestCorrupted{field, reason})
}

// session represent a persistent database session.
type session struct {
	// Need 64-bit alignment.
	stNextFileNum    int64 // current unused file number
	stJournalNum     int64 // current journal file number; need external synchronization
	stPrevJournalNum int64 // prev journal file number; no longer used; for compatibility with older version of leveldb
	stTempFileNum    int64
	stSeqNum         uint64 // last mem compacted seq; need external synchronization

	stor     *iStorage
	storLock storage.Locker
	o        *cachedOptions
	icmp     *iComparer
	tops     *tOps

	manifest       *journal.Writer
	manifestWriter storage.Writer
	manifestFd     storage.FileDesc

	stCompPtrs  []internalKey // compaction pointers; need external synchronization
	stVersion   *version      // current version
	ntVersionID int64         // next version id to assign
	refCh       chan *vTask
	relCh       chan *vTask
	deltaCh     chan *vDelta
	abandon     chan int64
	closeC      chan struct{}
	closeW      sync.WaitGroup
	vmu         sync.Mutex

	baseManifestSize int64 // manifest file size since last build

	// manifestErr is the error of the first failed commit, after which the
	// MANIFEST takes no more records; need external synchronization.
	manifestErr error

	// Testing fields
	fileRefCh chan chan map[int64]int // channel used to pass current reference stat
}

// Creates new initialized session instance.
func newSession(stor storage.Storage, o *opt.Options) (s *session, err error) {
	if stor == nil {
		return nil, os.ErrInvalid
	}
	storLock, err := stor.Lock()
	if err != nil {
		return
	}
	s = &session{
		stor:      newIStorage(stor),
		storLock:  storLock,
		refCh:     make(chan *vTask),
		relCh:     make(chan *vTask),
		deltaCh:   make(chan *vDelta),
		abandon:   make(chan int64),
		fileRefCh: make(chan chan map[int64]int),
		closeC:    make(chan struct{}),
	}
	s.setOptions(o)
	s.tops = newTableOps(s)

	s.closeW.Add(1)
	go s.refLoop()
	s.setVersion(nil, newVersion(s))
	s.log("log@legend F·NumFile S·FileSize N·Entry C·BadEntry B·BadBlock Ke·KeyError D·DroppedEntry L·Level Q·SeqNum T·TimeElapsed")
	return
}

// Close session.
func (s *session) close() {
	s.tops.close()
	if s.manifest != nil {
		s.manifest.Close()
	}
	if s.manifestWriter != nil {
		s.manifestWriter.Close()
	}
	s.manifest = nil
	s.manifestWriter = nil
	s.setVersion(nil, &version{s: s, closing: true, id: s.ntVersionID})

	// Close all background goroutines
	close(s.closeC)
	s.closeW.Wait()
}

// Release session lock.
func (s *session) release() {
	s.storLock.Unlock()
}

// Create a new database session; need external synchronization.
func (s *session) create() error {
	// create manifest
	return s.newManifest(nil, nil)
}

// cutRecord reads a MANIFEST record and notes whether the journal reader cut
// it short, which, as recovery reads strictly, it does only for a record the
// end of the file cuts off. Decoding reports that as a short read, like a record whose own
// content falls short, so it cannot tell the two apart.
type cutRecord struct {
	r   byteReader
	cut bool
}

func (c *cutRecord) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.cut = c.cut || err == io.ErrUnexpectedEOF
	return n, err
}

func (c *cutRecord) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	c.cut = c.cut || err == io.ErrUnexpectedEOF
	return b, err
}

// Recover a database session; need external synchronization.
func (s *session) recover() (err error) {
	defer func() {
		if os.IsNotExist(err) {
			// Don't return os.ErrNotExist if the underlying storage contains
			// other files that belong to LevelDB. So the DB won't get trashed.
			if fds, _ := s.stor.List(storage.TypeAll); len(fds) > 0 {
				err = &errors.ErrCorrupted{Err: errors.New("database entry point either missing or corrupted")}
			}
		}
	}()

	fd, err := s.stor.GetMeta()
	if err != nil {
		return
	}

	reader, err := s.stor.Open(fd)
	if err != nil {
		return
	}
	defer reader.Close()

	var (
		// Strict whatever the options, as in LevelDB: skipping a corrupted
		// record could leave out tables it added, for the open to remove.
		// Only a last record the end of the file cuts off, a commit that
		// never finished, is dropped.
		jr      = journal.NewReader(reader, dropper{s, fd}, true, true)
		rec     = &sessionRecord{} // the fields the records have set so far
		staging = s.stVersion.newStaging()

		// The highest sequence recorded. A MANIFEST written before commits
		// stopped lowering it can end below a sequence already in use.
		maxSeq uint64
	)
	for {
		var r io.Reader
		r, err = jr.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return errors.SetFd(err, fd)
		}

		// Decode each record on its own, and apply it only whole. The journal
		// reader hands a record out as it reads it, so one cut short, by a
		// commit that never finished or by corruption, has decoded fields by
		// the time it fails: a journal number among them made recovery skip
		// the journal whose batches the record's table never got to hold.
		one := &sessionRecord{}
		cr := &cutRecord{r: r.(byteReader)} // the journal reader's records are byte readers
		err = one.decode(cr)
		if err != nil {
			err = errors.SetFd(err, fd)
			// A record the end of the file cuts off is a commit that never
			// finished, which LevelDB drops too.
			if !cr.cut || !errors.IsCorrupted(err) {
				return
			}
			s.logf("manifest error: %v (skipped)", err)
			continue
		}
		// save compact pointers
		for _, r := range one.compPtrs {
			s.setCompPtr(r.level, r.ikey)
		}
		// commit record to version staging
		staging.commit(one)
		if one.has(recSeqNum) && one.seqNum > maxSeq {
			maxSeq = one.seqNum
		}
		// A later record's fields override an earlier one's.
		if one.has(recComparer) {
			rec.setComparer(one.comparer)
		}
		if one.has(recJournalNum) {
			rec.setJournalNum(one.journalNum)
		}
		if one.has(recPrevJournalNum) {
			rec.setPrevJournalNum(one.prevJournalNum)
		}
		if one.has(recNextFileNum) {
			rec.setNextFileNum(one.nextFileNum)
		}
		if one.has(recSeqNum) {
			rec.setSeqNum(one.seqNum)
		}
	}

	switch {
	case !rec.has(recComparer):
		return newErrManifestCorrupted(fd, "comparer", "missing")
	case rec.comparer != s.icmp.uName():
		return newErrManifestCorrupted(fd, "comparer", fmt.Sprintf("mismatch: want '%s', got '%s'", s.icmp.uName(), rec.comparer))
	case !rec.has(recNextFileNum):
		return newErrManifestCorrupted(fd, "next-file-num", "missing")
	case !rec.has(recJournalNum):
		return newErrManifestCorrupted(fd, "journal-file-num", "missing")
	case !rec.has(recSeqNum):
		return newErrManifestCorrupted(fd, "seq-num", "missing")
	case maxSeq > keyMaxSeq:
		// An internal key cannot hold it, so every read would panic.
		return newErrManifestCorrupted(fd, "seq-num", "past the largest sequence number")
	}

	s.manifestFd = fd
	s.setVersion(rec, staging.finish(false))
	s.setNextFileNum(rec.nextFileNum)
	rec.setSeqNum(maxSeq)
	s.recordCommited(rec)
	return nil
}

// shouldRebuildManifest returns whether manifest file should be rebuilt; need external synchronization.
func (s *session) shouldRebuildManifest() bool {
	if s.manifest == nil {
		return false
	}
	size := s.manifest.Size()
	// To throttle manifest rebuilding, check if manifest size grows enough (doubled)
	// since last build, in addition to size limit check.
	// See: https://github.com/syndtr/goleveldb/issues/413.
	return size >= s.baseManifestSize*2 && size >= s.o.GetMaxManifestFileSize()
}

// Commit session; need external synchronization.
func (s *session) commit(r *sessionRecord, trivial bool) (err error) {
	// A failed commit can leave a torn record at the end of the MANIFEST, and
	// the next open drops whatever follows one in its block. So once a commit
	// fails, nothing more is appended until the DB is reopened.
	if s.manifestErr != nil {
		return s.manifestErr
	}

	// Never record a lower sequence than the MANIFEST already holds, or
	// writes after a reopen would reuse sequence numbers in use.
	if r.has(recSeqNum) && r.seqNum < s.stSeqNum {
		r.setSeqNum(s.stSeqNum)
	}

	v := s.version()
	defer v.release()

	// spawn new version based on current version
	nv := v.spawn(r, trivial)

	// abandon useless version id to prevent blocking version processing loop.
	defer func() {
		if err != nil {
			s.abandon <- nv.id
			s.logf("commit@abandon useless vid D%d", nv.id)
		}
	}()

	if s.manifest == nil {
		// manifest journal writer not yet created, create one
		err = s.newManifest(r, nv)
	} else if s.shouldRebuildManifest() {
		// Start the new MANIFEST with a snapshot of the current version and
		// append the record to it, as LevelDB does. Passing the record to
		// newManifest would add every table of the version to it and leak
		// them through setVersion (syndtr/goleveldb#409), and a snapshot of
		// nv without it loses its journal number, sequence number and
		// compaction pointers.
		err = s.newManifest(nil, v)
		if err == nil {
			err = s.flushManifest(r)
		}
	} else {
		err = s.flushManifest(r)
	}

	// finally, apply new version if no error rise
	if err == nil {
		s.setVersion(r, nv)
	} else {
		s.manifestErr = err
	}

	return
}
