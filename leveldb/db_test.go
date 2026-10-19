// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"container/list"
	crand "crypto/rand"
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/onsi/gomega"

	"github.com/gcash/goleveldb/leveldb/comparer"
	"github.com/gcash/goleveldb/leveldb/errors"
	"github.com/gcash/goleveldb/leveldb/filter"
	"github.com/gcash/goleveldb/leveldb/iterator"
	"github.com/gcash/goleveldb/leveldb/journal"
	"github.com/gcash/goleveldb/leveldb/opt"
	"github.com/gcash/goleveldb/leveldb/storage"
	"github.com/gcash/goleveldb/leveldb/testutil"
	"github.com/gcash/goleveldb/leveldb/util"
)

func tkey(i int) []byte {
	return []byte(fmt.Sprintf("%016d", i))
}

func tval(seed, n int) []byte {
	r := rand.New(rand.NewSource(int64(seed)))
	return randomString(r, n)
}

func testingLogger(t *testing.T) func(log string) {
	return func(log string) {
		t.Log(log)
	}
}

func testingPreserveOnFailed(t *testing.T) func() (preserve bool, err error) {
	return func() (preserve bool, err error) {
		preserve = t.Failed()
		return
	}
}

type dbHarness struct {
	t *testing.T

	stor *testutil.Storage
	db   *DB
	o    *opt.Options
	ro   *opt.ReadOptions
	wo   *opt.WriteOptions
}

func newDbHarnessWopt(t *testing.T, o *opt.Options) *dbHarness {
	h := new(dbHarness)
	h.init(t, o)
	return h
}

func newDbHarness(t *testing.T) *dbHarness {
	return newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true})
}

func (h *dbHarness) init(t *testing.T, o *opt.Options) {
	gomega.RegisterTestingT(t)
	h.t = t
	h.stor = testutil.NewStorage()
	h.stor.OnLog(testingLogger(t))
	h.stor.OnClose(testingPreserveOnFailed(t))
	h.o = o
	h.ro = nil
	h.wo = nil

	if err := h.openDB0(); err != nil {
		// So that it will come after fatal message.
		defer h.stor.Close()
		h.t.Fatal("Open (init): got error: ", err)
	}
}

func (h *dbHarness) openDB0() (err error) {
	h.t.Log("opening DB")
	h.db, err = Open(h.stor, h.o)
	return
}

func (h *dbHarness) openDB() {
	if err := h.openDB0(); err != nil {
		h.t.Fatal("Open: got error: ", err)
	}
}

func (h *dbHarness) closeDB0() error {
	h.t.Log("closing DB")
	return h.db.Close()
}

// closeErrExpected reports whether err is an error Close may return in a
// test: ErrClosed for a second Close, or the error that stopped writes, which
// the tests that stop them expect.
func closeErrExpected(err error) bool {
	return err == nil || err == ErrClosed || stderrors.Is(err, ErrWritesStopped)
}

func (h *dbHarness) closeDB() {
	if h.db != nil {
		if err := h.closeDB0(); !closeErrExpected(err) {
			h.t.Error("Close: got error: ", err)
		}
	}
	h.stor.CloseCheck()
	runtime.GC()
}

func (h *dbHarness) reopenDB() {
	if h.db != nil {
		h.closeDB()
	}
	h.openDB()
}

func (h *dbHarness) close() {
	if h.db != nil {
		if err := h.closeDB0(); !closeErrExpected(err) {
			h.t.Error("Close: got error: ", err)
		}
		h.db = nil
	}
	h.stor.Close()
	h.stor = nil
	runtime.GC()
}

func (h *dbHarness) openAssert(want bool) {
	db, err := Open(h.stor, h.o)
	if err != nil {
		if want {
			h.t.Error("Open: assert: got error: ", err)
		} else {
			h.t.Log("Open: assert: got error (expected): ", err)
		}
	} else {
		if !want {
			h.t.Error("Open: assert: expect error")
		}
		db.Close()
	}
}

func (h *dbHarness) write(batch *Batch) {
	if err := h.db.Write(batch, h.wo); err != nil {
		h.t.Error("Write: got error: ", err)
	}
}

func (h *dbHarness) put(key, value string) {
	if err := h.db.Put([]byte(key), []byte(value), h.wo); err != nil {
		h.t.Error("Put: got error: ", err)
	}
}

func (h *dbHarness) putMulti(n int, low, hi string) {
	for i := 0; i < n; i++ {
		h.put(low, "begin")
		h.put(hi, "end")
		h.compactMem()
	}
}

func (h *dbHarness) maxNextLevelOverlappingBytes(want int64) {
	t := h.t
	db := h.db

	var (
		maxOverlaps int64
		maxLevel    int
	)
	v := db.s.version()
	if len(v.levels) > 2 {
		for i, tt := range v.levels[1 : len(v.levels)-1] {
			level := i + 1
			next := v.levels[level+1]
			for _, t := range tt {
				r := next.getOverlaps(nil, db.s.icmp, t.imin.ukey(), t.imax.ukey(), false)
				sum := r.size()
				if sum > maxOverlaps {
					maxOverlaps = sum
					maxLevel = level
				}
			}
		}
	}
	v.release()

	if maxOverlaps > want {
		t.Errorf("next level most overlapping bytes is more than %d, got=%d level=%d", want, maxOverlaps, maxLevel)
	} else {
		t.Logf("next level most overlapping bytes is %d, level=%d want=%d", maxOverlaps, maxLevel, want)
	}
}

func (h *dbHarness) delete(key string) {
	t := h.t
	db := h.db

	err := db.Delete([]byte(key), h.wo)
	if err != nil {
		t.Error("Delete: got error: ", err)
	}
}

func (h *dbHarness) assertNumKeys(want int) {
	iter := h.db.NewIterator(nil, h.ro)
	defer iter.Release()
	got := 0
	for iter.Next() {
		got++
	}
	if err := iter.Error(); err != nil {
		h.t.Error("assertNumKeys: ", err)
	}
	if want != got {
		h.t.Errorf("assertNumKeys: want=%d got=%d", want, got)
	}
}

func (h *dbHarness) getr(db Reader, key string, expectFound bool) (found bool, v []byte) {
	t := h.t
	v, err := db.Get([]byte(key), h.ro)
	switch err {
	case ErrNotFound:
		if expectFound {
			t.Errorf("Get: key '%s' not found, want found", key)
		}
	case nil:
		found = true
		if !expectFound {
			t.Errorf("Get: key '%s' found, want not found", key)
		}
	default:
		t.Error("Get: got error: ", err)
	}
	return
}

func (h *dbHarness) get(key string, expectFound bool) (found bool, v []byte) {
	return h.getr(h.db, key, expectFound)
}

func (h *dbHarness) getValr(db Reader, key, value string) {
	t := h.t
	found, r := h.getr(db, key, true)
	if !found {
		return
	}
	rval := string(r)
	if rval != value {
		t.Errorf("Get: invalid value, got '%s', want '%s'", rval, value)
	}
}

func (h *dbHarness) getVal(key, value string) {
	h.getValr(h.db, key, value)
}

func (h *dbHarness) allEntriesFor(key, want string) {
	t := h.t
	db := h.db
	s := db.s

	ikey := makeInternalKey(nil, []byte(key), keyMaxSeq, keyTypeVal)
	iter := db.newRawIterator(nil, nil, nil, nil)
	if !iter.Seek(ikey) && iter.Error() != nil {
		t.Error("AllEntries: error during seek, err: ", iter.Error())
		return
	}
	res := "[ "
	first := true
	for iter.Valid() {
		if ukey, _, kt, kerr := parseInternalKey(iter.Key()); kerr == nil {
			if s.icmp.uCompare(ikey.ukey(), ukey) != 0 {
				break
			}
			if !first {
				res += ", "
			}
			first = false
			switch kt {
			case keyTypeVal:
				res += string(iter.Value())
			case keyTypeDel:
				res += "DEL"
			}
		} else {
			if !first {
				res += ", "
			}
			first = false
			res += "CORRUPTED"
		}
		iter.Next()
	}
	if !first {
		res += " "
	}
	res += "]"
	if res != want {
		t.Errorf("AllEntries: assert failed for key %q, got=%q want=%q", key, res, want)
	}
}

// Return a string that contains all key,value pairs in order,
// formatted like "(k1->v1)(k2->v2)".
func (h *dbHarness) getKeyVal(want string) {
	t := h.t
	db := h.db

	s, err := db.GetSnapshot()
	if err != nil {
		t.Fatal("GetSnapshot: got error: ", err)
	}
	res := ""
	iter := s.NewIterator(nil, nil)
	for iter.Next() {
		res += fmt.Sprintf("(%s->%s)", string(iter.Key()), string(iter.Value()))
	}
	iter.Release()

	if res != want {
		t.Errorf("GetKeyVal: invalid key/value pair, got=%q want=%q", res, want)
	}
	s.Release()
}

func (h *dbHarness) waitCompaction() {
	t := h.t
	db := h.db
	if err := db.compTriggerWait(db.tcompCmdC); err != nil {
		t.Error("compaction error: ", err)
	}
}

func (h *dbHarness) waitMemCompaction() {
	t := h.t
	db := h.db

	if err := db.compTriggerWait(db.mcompCmdC); err != nil {
		t.Error("compaction error: ", err)
	}
}

func (h *dbHarness) compactMem() {
	t := h.t
	db := h.db

	t.Log("starting memdb compaction")

	db.writeLockC <- struct{}{}
	defer func() {
		<-db.writeLockC
	}()

	if _, err := db.rotateMem(0, true); err != nil {
		t.Error("compaction error: ", err)
	}

	if h.totalTables() == 0 {
		t.Error("zero tables after mem compaction")
	}

	t.Log("memdb compaction done")
}

func (h *dbHarness) compactRangeAtErr(level int, min, max string, wanterr bool) {
	t := h.t
	db := h.db

	var _min, _max []byte
	if min != "" {
		_min = []byte(min)
	}
	if max != "" {
		_max = []byte(max)
	}

	t.Logf("starting table range compaction: level=%d, min=%q, max=%q", level, min, max)

	if err := db.compTriggerRange(db.tcompCmdC, level, _min, _max); err != nil {
		if wanterr {
			t.Log("CompactRangeAt: got error (expected): ", err)
		} else {
			t.Error("CompactRangeAt: got error: ", err)
		}
	} else if wanterr {
		t.Error("CompactRangeAt: expect error")
	}

	t.Log("table range compaction done")
}

func (h *dbHarness) compactRangeAt(level int, min, max string) {
	h.compactRangeAtErr(level, min, max, false)
}

func (h *dbHarness) compactRange(min, max string) {
	t := h.t
	db := h.db

	t.Logf("starting DB range compaction: min=%q, max=%q", min, max)

	var r util.Range
	if min != "" {
		r.Start = []byte(min)
	}
	if max != "" {
		r.Limit = []byte(max)
	}
	if err := db.CompactRange(r); err != nil {
		t.Error("CompactRange: got error: ", err)
	}

	t.Log("DB range compaction done")
}

func (h *dbHarness) sizeOf(start, limit string) int64 {
	sz, err := h.db.SizeOf([]util.Range{
		{Start: []byte(start), Limit: []byte(limit)},
	})
	if err != nil {
		h.t.Error("SizeOf: got error: ", err)
	}
	return sz.Sum()
}

func (h *dbHarness) sizeAssert(start, limit string, low, hi int64) {
	sz := h.sizeOf(start, limit)
	if sz < low || sz > hi {
		h.t.Errorf("sizeOf %q to %q not in range, want %d - %d, got %d",
			shorten(start), shorten(limit), low, hi, sz)
	}
}

func (h *dbHarness) getSnapshot() (s *Snapshot) {
	s, err := h.db.GetSnapshot()
	if err != nil {
		h.t.Fatal("GetSnapshot: got error: ", err)
	}
	return
}

func (h *dbHarness) getTablesPerLevel() string {
	res := ""
	nz := 0
	v := h.db.s.version()
	for level, tables := range v.levels {
		if level > 0 {
			res += ","
		}
		res += fmt.Sprint(len(tables))
		if len(tables) > 0 {
			nz = len(res)
		}
	}
	v.release()
	return res[:nz]
}

func (h *dbHarness) tablesPerLevel(want string) {
	res := h.getTablesPerLevel()
	if res != want {
		h.t.Errorf("invalid tables len, want=%s, got=%s", want, res)
	}
}

func (h *dbHarness) totalTables() (n int) {
	v := h.db.s.version()
	for _, tables := range v.levels {
		n += len(tables)
	}
	v.release()
	return
}

type keyValue interface {
	Key() []byte
	Value() []byte
}

func testKeyVal(t *testing.T, kv keyValue, want string) {
	res := string(kv.Key()) + "->" + string(kv.Value())
	if res != want {
		t.Errorf("invalid key/value, want=%q, got=%q", want, res)
	}
}

func numKey(num int) string {
	return fmt.Sprintf("key%06d", num)
}

var testingBloomFilter = filter.NewBloomFilter(10)

func truno(t *testing.T, o *opt.Options, f func(h *dbHarness)) {
	for i := 0; i < 4; i++ {
		func() {
			switch i {
			case 0:
			case 1:
				if o == nil {
					o = &opt.Options{
						DisableLargeBatchTransaction: true,
						Filter:                       testingBloomFilter,
					}
				} else {
					old := o
					o = &opt.Options{}
					*o = *old
					o.Filter = testingBloomFilter
				}
			case 2:
				if o == nil {
					o = &opt.Options{
						DisableLargeBatchTransaction: true,
						Compression:                  opt.NoCompression,
					}
				} else {
					old := o
					o = &opt.Options{}
					*o = *old
					o.Compression = opt.NoCompression
				}
			}
			h := newDbHarnessWopt(t, o)
			defer h.close()
			if i == 3 {
				h.reopenDB()
			}
			f(h)
		}()
	}
}

func trun(t *testing.T, f func(h *dbHarness)) {
	truno(t, nil, f)
}

func testAligned(t *testing.T, name string, offset uintptr) {
	if offset%8 != 0 {
		t.Errorf("field %s offset is not 64-bit aligned", name)
	}
}

func Test_FieldsAligned(t *testing.T) {
	p1 := new(DB) //nolint:staticcheck
	testAligned(t, "DB.seq", unsafe.Offsetof(p1.seq))
	p2 := new(session) //nolint:staticcheck
	testAligned(t, "session.stNextFileNum", unsafe.Offsetof(p2.stNextFileNum))
	testAligned(t, "session.stJournalNum", unsafe.Offsetof(p2.stJournalNum))
	testAligned(t, "session.stPrevJournalNum", unsafe.Offsetof(p2.stPrevJournalNum))
	testAligned(t, "session.stSeqNum", unsafe.Offsetof(p2.stSeqNum))
}

func TestDB_Locking(t *testing.T) {
	h := newDbHarness(t)
	defer h.stor.Close()
	h.openAssert(false)
	h.closeDB()
	h.openAssert(true)
}

func TestDB_Empty(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.get("foo", false)

		h.reopenDB()
		h.get("foo", false)
	})
}

func TestDB_ReadWrite(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		h.getVal("foo", "v1")
		h.put("bar", "v2")
		h.put("foo", "v3")
		h.getVal("foo", "v3")
		h.getVal("bar", "v2")

		h.reopenDB()
		h.getVal("foo", "v3")
		h.getVal("bar", "v2")
	})
}

func TestDB_PutDeleteGet(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		h.getVal("foo", "v1")
		h.put("foo", "v2")
		h.getVal("foo", "v2")
		h.delete("foo")
		h.get("foo", false)

		h.reopenDB()
		h.get("foo", false)
	})
}

func TestDB_EmptyBatch(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.get("foo", false)
	err := h.db.Write(new(Batch), h.wo)
	if err != nil {
		t.Error("writing empty batch yield error: ", err)
	}
	h.get("foo", false)
}

func TestDB_GetFromFrozen(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  100100,
	})
	defer h.close()

	h.put("foo", "v1")
	h.getVal("foo", "v1")

	h.stor.Stall(testutil.ModeSync, storage.TypeTable) // Block sync calls
	h.put("k1", strings.Repeat("x", 100000))           // Fill memtable
	h.put("k2", strings.Repeat("y", 100000))           // Trigger compaction
	for i := 0; h.db.getFrozenMem() == nil && i < 100; i++ {
		time.Sleep(10 * time.Microsecond)
	}
	if h.db.getFrozenMem() == nil {
		h.stor.Release(testutil.ModeSync, storage.TypeTable)
		t.Fatal("No frozen mem")
	}
	h.getVal("foo", "v1")
	h.stor.Release(testutil.ModeSync, storage.TypeTable) // Release sync calls

	h.reopenDB()
	h.getVal("foo", "v1")
	h.get("k1", true)
	h.get("k2", true)
}

func TestDB_GetFromTable(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		h.compactMem()
		h.getVal("foo", "v1")
	})
}

func TestDB_GetSnapshot(t *testing.T) {
	trun(t, func(h *dbHarness) {
		bar := strings.Repeat("b", 200)
		h.put("foo", "v1")
		h.put(bar, "v1")

		snap, err := h.db.GetSnapshot()
		if err != nil {
			t.Fatal("GetSnapshot: got error: ", err)
		}

		h.put("foo", "v2")
		h.put(bar, "v2")

		h.getVal("foo", "v2")
		h.getVal(bar, "v2")
		h.getValr(snap, "foo", "v1")
		h.getValr(snap, bar, "v1")

		h.compactMem()

		h.getVal("foo", "v2")
		h.getVal(bar, "v2")
		h.getValr(snap, "foo", "v1")
		h.getValr(snap, bar, "v1")

		snap.Release()

		h.reopenDB()
		h.getVal("foo", "v2")
		h.getVal(bar, "v2")
	})
}

func TestDB_GetLevel0Ordering(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.db.memdbMaxLevel = 2

		for i := 0; i < 4; i++ {
			h.put("bar", fmt.Sprintf("b%d", i))
			h.put("foo", fmt.Sprintf("v%d", i))
			h.compactMem()
		}
		h.getVal("foo", "v3")
		h.getVal("bar", "b3")

		v := h.db.s.version()
		t0len := v.tLen(0)
		v.release()
		if t0len < 2 {
			t.Errorf("level-0 tables is less than 2, got %d", t0len)
		}

		h.reopenDB()
		h.getVal("foo", "v3")
		h.getVal("bar", "b3")
	})
}

func TestDB_GetOrderedByLevels(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		h.compactMem()
		h.compactRange("a", "z")
		h.getVal("foo", "v1")
		h.put("foo", "v2")
		h.compactMem()
		h.getVal("foo", "v2")
	})
}

func TestDB_GetPicksCorrectFile(t *testing.T) {
	trun(t, func(h *dbHarness) {
		// Arrange to have multiple files in a non-level-0 level.
		h.put("a", "va")
		h.compactMem()
		h.compactRange("a", "b")
		h.put("x", "vx")
		h.compactMem()
		h.compactRange("x", "y")
		h.put("f", "vf")
		h.compactMem()
		h.compactRange("f", "g")

		h.getVal("a", "va")
		h.getVal("f", "vf")
		h.getVal("x", "vx")

		h.compactRange("", "")
		h.getVal("a", "va")
		h.getVal("f", "vf")
		h.getVal("x", "vx")
	})
}

func TestDB_GetEncountersEmptyLevel(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.db.memdbMaxLevel = 2

		// Arrange for the following to happen:
		//   * sstable A in level 0
		//   * nothing in level 1
		//   * sstable B in level 2
		// Then do enough Get() calls to arrange for an automatic compaction
		// of sstable A.  A bug would cause the compaction to be marked as
		// occurring at level 1 (instead of the correct level 0).

		// Step 1: First place sstables in levels 0 and 2
		for i := 0; ; i++ {
			if i >= 100 {
				t.Fatal("could not fill levels-0 and level-2")
			}
			v := h.db.s.version()
			if v.tLen(0) > 0 && v.tLen(2) > 0 {
				v.release()
				break
			}
			v.release()
			h.put("a", "begin")
			h.put("z", "end")
			h.compactMem()

			h.getVal("a", "begin")
			h.getVal("z", "end")
		}

		// Step 2: clear level 1 if necessary.
		h.compactRangeAt(1, "", "")
		h.tablesPerLevel("1,0,1")

		h.getVal("a", "begin")
		h.getVal("z", "end")

		// Step 3: read a bunch of times
		for i := 0; i < 200; i++ {
			h.get("missing", false)
		}

		// Step 4: Wait for compaction to finish
		h.waitCompaction()

		v := h.db.s.version()
		if v.tLen(0) > 0 {
			t.Errorf("level-0 tables more than 0, got %d", v.tLen(0))
		}
		v.release()

		h.getVal("a", "begin")
		h.getVal("z", "end")
	})
}

func TestDB_IterMultiWithDelete(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("a", "va")
		h.put("b", "vb")
		h.put("c", "vc")
		h.delete("b")
		h.get("b", false)

		iter := h.db.NewIterator(nil, nil)
		iter.Seek([]byte("c"))
		testKeyVal(t, iter, "c->vc")
		iter.Prev()
		testKeyVal(t, iter, "a->va")
		iter.Release()

		h.compactMem()

		iter = h.db.NewIterator(nil, nil)
		iter.Seek([]byte("c"))
		testKeyVal(t, iter, "c->vc")
		iter.Prev()
		testKeyVal(t, iter, "a->va")
		iter.Release()
	})
}

func TestDB_IteratorPinsRef(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("foo", "hello")

	// Get iterator that will yield the current contents of the DB.
	iter := h.db.NewIterator(nil, nil)

	// Write to force compactions
	h.put("foo", "newvalue1")
	for i := 0; i < 100; i++ {
		h.put(numKey(i), strings.Repeat(fmt.Sprintf("v%09d", i), 100000/10))
	}
	h.put("foo", "newvalue2")

	iter.First()
	testKeyVal(t, iter, "foo->hello")
	if iter.Next() {
		t.Errorf("expect eof")
	}
	iter.Release()
}

func TestDB_Recover(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		h.put("baz", "v5")

		h.reopenDB()
		h.getVal("foo", "v1")

		h.getVal("foo", "v1")
		h.getVal("baz", "v5")
		h.put("bar", "v2")
		h.put("foo", "v3")

		h.reopenDB()
		h.getVal("foo", "v3")
		h.put("foo", "v4")
		h.getVal("foo", "v4")
		h.getVal("bar", "v2")
		h.getVal("baz", "v5")
	})
}

func TestDB_RecoverWithEmptyJournal(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		h.put("foo", "v2")

		h.reopenDB()
		h.reopenDB()
		h.put("foo", "v3")

		h.reopenDB()
		h.getVal("foo", "v3")
	})
}

func TestDB_RecoverDuringMemtableCompaction(t *testing.T) {
	truno(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 1000000}, func(h *dbHarness) {

		h.stor.Stall(testutil.ModeSync, storage.TypeTable)
		h.put("big1", strings.Repeat("x", 10000000))
		h.put("big2", strings.Repeat("y", 1000))
		h.put("bar", "v2")
		h.stor.Release(testutil.ModeSync, storage.TypeTable)

		h.reopenDB()
		h.getVal("bar", "v2")
		h.getVal("big1", strings.Repeat("x", 10000000))
		h.getVal("big2", strings.Repeat("y", 1000))
	})
}

func TestDB_MinorCompactionsHappen(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 10000})
	defer h.close()

	n := 500

	key := func(i int) string {
		return fmt.Sprintf("key%06d", i)
	}

	for i := 0; i < n; i++ {
		h.put(key(i), key(i)+strings.Repeat("v", 1000))
	}

	for i := 0; i < n; i++ {
		h.getVal(key(i), key(i)+strings.Repeat("v", 1000))
	}

	h.reopenDB()
	for i := 0; i < n; i++ {
		h.getVal(key(i), key(i)+strings.Repeat("v", 1000))
	}
}

func TestDB_RecoverWithLargeJournal(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("big1", strings.Repeat("1", 200000))
	h.put("big2", strings.Repeat("2", 200000))
	h.put("small3", strings.Repeat("3", 10))
	h.put("small4", strings.Repeat("4", 10))
	h.tablesPerLevel("")

	// Make sure that if we re-open with a small write buffer size that
	// we flush table files in the middle of a large journal file.
	h.o.WriteBuffer = 100000
	h.reopenDB()
	h.getVal("big1", strings.Repeat("1", 200000))
	h.getVal("big2", strings.Repeat("2", 200000))
	h.getVal("small3", strings.Repeat("3", 10))
	h.getVal("small4", strings.Repeat("4", 10))
	v := h.db.s.version()
	if v.tLen(0) <= 1 {
		t.Errorf("tables-0 less than one")
	}
	v.release()
}

func TestDB_CompactionsGenerateMultipleFiles(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  10000000,
		Compression:                  opt.NoCompression,
	})
	defer h.close()

	v := h.db.s.version()
	if v.tLen(0) > 0 {
		t.Errorf("level-0 tables more than 0, got %d", v.tLen(0))
	}
	v.release()

	n := 80

	// Write 8MB (80 values, each 100K)
	for i := 0; i < n; i++ {
		h.put(numKey(i), strings.Repeat(fmt.Sprintf("v%09d", i), 100000/10))
	}

	// Reopening moves updates to level-0
	h.reopenDB()
	h.compactRangeAt(0, "", "")

	v = h.db.s.version()
	if v.tLen(0) > 0 {
		t.Errorf("level-0 tables more than 0, got %d", v.tLen(0))
	}
	if v.tLen(1) <= 1 {
		t.Errorf("level-1 tables less than 1, got %d", v.tLen(1))
	}
	v.release()

	for i := 0; i < n; i++ {
		h.getVal(numKey(i), strings.Repeat(fmt.Sprintf("v%09d", i), 100000/10))
	}
}

func TestDB_RepeatedWritesToSameKey(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 100000})
	defer h.close()

	maxTables := h.o.GetWriteL0PauseTrigger() + 7

	value := strings.Repeat("v", 2*h.o.GetWriteBuffer())
	for i := 0; i < 5*maxTables; i++ {
		h.put("key", value)
		n := h.totalTables()
		if n > maxTables {
			t.Errorf("total tables exceed %d, got=%d, iter=%d", maxTables, n, i)
		}
	}
}

func TestDB_RepeatedWritesToSameKeyAfterReopen(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  100000,
	})
	defer h.close()

	h.reopenDB()

	maxTables := h.o.GetWriteL0PauseTrigger() + 7

	value := strings.Repeat("v", 2*h.o.GetWriteBuffer())
	for i := 0; i < 5*maxTables; i++ {
		h.put("key", value)
		n := h.totalTables()
		if n > maxTables {
			t.Errorf("total tables exceed %d, got=%d, iter=%d", maxTables, n, i)
		}
	}
}

func TestDB_SparseMerge(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, Compression: opt.NoCompression})
	defer h.close()

	h.putMulti(7, "A", "Z")

	// Suppose there is:
	//    small amount of data with prefix A
	//    large amount of data with prefix B
	//    small amount of data with prefix C
	// and that recent updates have made small changes to all three prefixes.
	// Check that we do not do a compaction that merges all of B in one shot.
	h.put("A", "va")
	value := strings.Repeat("x", 1000)
	for i := 0; i < 100000; i++ {
		h.put(fmt.Sprintf("B%010d", i), value)
	}
	h.put("C", "vc")
	h.compactMem()
	h.compactRangeAt(0, "", "")
	h.waitCompaction()

	// Make sparse update
	h.put("A", "va2")
	h.put("B100", "bvalue2")
	h.put("C", "vc2")
	h.compactMem()

	h.waitCompaction()
	h.maxNextLevelOverlappingBytes(20 * 1048576)
	h.compactRangeAt(0, "", "")
	h.waitCompaction()
	h.maxNextLevelOverlappingBytes(20 * 1048576)
	h.compactRangeAt(1, "", "")
	h.waitCompaction()
	h.maxNextLevelOverlappingBytes(20 * 1048576)
}

func TestDB_SizeOf(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
		WriteBuffer:                  10000000,
	})
	defer h.close()

	h.sizeAssert("", "xyz", 0, 0)
	h.reopenDB()
	h.sizeAssert("", "xyz", 0, 0)

	// Write 8MB (80 values, each 100K)
	n := 80
	s1 := 100000
	s2 := 105000

	for i := 0; i < n; i++ {
		h.put(numKey(i), strings.Repeat(fmt.Sprintf("v%09d", i), s1/10))
	}

	// 0 because SizeOf() does not account for memtable space
	h.sizeAssert("", numKey(50), 0, 0)

	for r := 0; r < 3; r++ {
		h.reopenDB()

		for cs := 0; cs < n; cs += 10 {
			for i := 0; i < n; i += 10 {
				h.sizeAssert("", numKey(i), int64(s1*i), int64(s2*i))
				h.sizeAssert("", numKey(i)+".suffix", int64(s1*(i+1)), int64(s2*(i+1)))
				h.sizeAssert(numKey(i), numKey(i+10), int64(s1*10), int64(s2*10))
			}

			h.sizeAssert("", numKey(50), int64(s1*50), int64(s2*50))
			h.sizeAssert("", numKey(50)+".suffix", int64(s1*50), int64(s2*50))

			h.compactRangeAt(0, numKey(cs), numKey(cs+9))
		}

		v := h.db.s.version()
		if v.tLen(0) != 0 {
			t.Errorf("level-0 tables was not zero, got %d", v.tLen(0))
		}
		if v.tLen(1) == 0 {
			t.Error("level-1 tables was zero")
		}
		v.release()
	}
}

func TestDB_SizeOf_MixOfSmallAndLarge(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
	})
	defer h.close()

	sizes := []int64{
		10000,
		10000,
		100000,
		10000,
		100000,
		10000,
		300000,
		10000,
	}

	for i, n := range sizes {
		h.put(numKey(i), strings.Repeat(fmt.Sprintf("v%09d", i), int(n)/10))
	}

	for r := 0; r < 3; r++ {
		h.reopenDB()

		var x int64
		for i, n := range sizes {
			y := x
			if i > 0 {
				y += 1000
			}
			h.sizeAssert("", numKey(i), x, y)
			x += n
		}

		h.sizeAssert(numKey(3), numKey(5), 110000, 111000)

		h.compactRangeAt(0, "", "")
	}
}

func TestDB_Snapshot(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.put("foo", "v1")
		s1 := h.getSnapshot()
		h.put("foo", "v2")
		s2 := h.getSnapshot()
		h.put("foo", "v3")
		s3 := h.getSnapshot()
		h.put("foo", "v4")

		h.getValr(s1, "foo", "v1")
		h.getValr(s2, "foo", "v2")
		h.getValr(s3, "foo", "v3")
		h.getVal("foo", "v4")

		s3.Release()
		h.getValr(s1, "foo", "v1")
		h.getValr(s2, "foo", "v2")
		h.getVal("foo", "v4")

		s1.Release()
		h.getValr(s2, "foo", "v2")
		h.getVal("foo", "v4")

		s2.Release()
		h.getVal("foo", "v4")
	})
}

func TestDB_SnapshotList(t *testing.T) {
	db := &DB{snapsList: list.New()}
	e0a := db.acquireSnapshot()
	e0b := db.acquireSnapshot()
	db.seq = 1
	e1 := db.acquireSnapshot()
	db.seq = 2
	e2 := db.acquireSnapshot()

	if db.minSeq() != 0 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
	db.releaseSnapshot(e0a)
	if db.minSeq() != 0 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
	db.releaseSnapshot(e2)
	if db.minSeq() != 0 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
	db.releaseSnapshot(e0b)
	if db.minSeq() != 1 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
	e2 = db.acquireSnapshot()
	if db.minSeq() != 1 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
	db.releaseSnapshot(e1)
	if db.minSeq() != 2 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
	db.releaseSnapshot(e2)
	if db.minSeq() != 2 {
		t.Fatalf("invalid sequence number, got=%d", db.minSeq())
	}
}

func TestDB_HiddenValuesAreRemoved(t *testing.T) {
	trun(t, func(h *dbHarness) {
		s := h.db.s

		m := 2
		h.db.memdbMaxLevel = m

		h.put("foo", "v1")
		h.compactMem()
		v := s.version()
		num := v.tLen(m)
		v.release()
		if num != 1 {
			t.Errorf("invalid level-%d len, want=1 got=%d", m, num)
		}

		// Place a table at level last-1 to prevent merging with preceding mutation
		h.put("a", "begin")
		h.put("z", "end")
		h.compactMem()
		v = s.version()
		if v.tLen(m) != 1 {
			t.Errorf("invalid level-%d len, want=1 got=%d", m, v.tLen(m))
		}
		if v.tLen(m-1) != 1 {
			t.Errorf("invalid level-%d len, want=1 got=%d", m-1, v.tLen(m-1))
		}
		v.release()

		h.delete("foo")
		h.put("foo", "v2")
		h.allEntriesFor("foo", "[ v2, DEL, v1 ]")
		h.compactMem()
		h.allEntriesFor("foo", "[ v2, DEL, v1 ]")
		h.compactRangeAt(m-2, "", "z")
		// DEL eliminated, but v1 remains because we aren't compacting that level
		// (DEL can be eliminated because v2 hides v1).
		h.allEntriesFor("foo", "[ v2, v1 ]")
		h.compactRangeAt(m-1, "", "")
		// Merging last-1 w/ last, so we are the base level for "foo", so
		// DEL is removed.  (as is v1).
		h.allEntriesFor("foo", "[ v2 ]")
	})
}

func TestDB_DeletionMarkers2(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()
	s := h.db.s

	m := 2
	h.db.memdbMaxLevel = m

	h.put("foo", "v1")
	h.compactMem()
	v := s.version()
	num := v.tLen(m)
	v.release()
	if num != 1 {
		t.Errorf("invalid level-%d len, want=1 got=%d", m, num)
	}

	// Place a table at level last-1 to prevent merging with preceding mutation
	h.put("a", "begin")
	h.put("z", "end")
	h.compactMem()
	v = s.version()
	if v.tLen(m) != 1 {
		t.Errorf("invalid level-%d len, want=1 got=%d", m, v.tLen(m))
	}
	if v.tLen(m-1) != 1 {
		t.Errorf("invalid level-%d len, want=1 got=%d", m-1, v.tLen(m-1))
	}
	v.release()

	h.delete("foo")
	h.allEntriesFor("foo", "[ DEL, v1 ]")
	h.compactMem() // Moves to level last-2
	h.allEntriesFor("foo", "[ DEL, v1 ]")
	h.compactRangeAt(m-2, "", "")
	// DEL kept: "last" file overlaps
	h.allEntriesFor("foo", "[ DEL, v1 ]")
	h.compactRangeAt(m-1, "", "")
	// Merging last-1 w/ last, so we are the base level for "foo", so
	// DEL is removed.  (as is v1).
	h.allEntriesFor("foo", "[ ]")
}

func TestDB_CompactionTableOpenError(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		OpenFilesCacheCapacity:       -1,
	})
	defer h.close()

	h.db.memdbMaxLevel = 2

	im := 10
	jm := 10
	for r := 0; r < 2; r++ {
		for i := 0; i < im; i++ {
			for j := 0; j < jm; j++ {
				h.put(fmt.Sprintf("k%d,%d", i, j), fmt.Sprintf("v%d,%d", i, j))
			}
			h.compactMem()
		}
	}

	if n := h.totalTables(); n != im*2 {
		t.Errorf("total tables is %d, want %d", n, im*2)
	}

	h.stor.EmulateError(testutil.ModeOpen, storage.TypeTable, errors.New("open error during table compaction"))
	go func() {
		if err := h.db.CompactRange(util.Range{}); err != nil && err != ErrClosed {
			t.Error("CompactRange error: ", err)
		}
	}()
	if err := h.db.compTriggerWait(h.db.tcompCmdC); err != nil {
		t.Log("compaction error: ", err)
	}
	if err := h.closeDB0(); err != nil {
		t.Error("Close: got error: ", err)
	}
	h.openDB()
	h.stor.EmulateError(testutil.ModeOpen, storage.TypeTable, nil)

	for i := 0; i < im; i++ {
		for j := 0; j < jm; j++ {
			h.getVal(fmt.Sprintf("k%d,%d", i, j), fmt.Sprintf("v%d,%d", i, j))
		}
	}
}

func TestDB_OverlapInLevel0(t *testing.T) {
	trun(t, func(h *dbHarness) {
		h.db.memdbMaxLevel = 2

		// Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
		h.put("100", "v100")
		h.put("999", "v999")
		h.compactMem()
		h.delete("100")
		h.delete("999")
		h.compactMem()
		h.tablesPerLevel("0,1,1")

		// Make files spanning the following ranges in level-0:
		//  files[0]  200 .. 900
		//  files[1]  300 .. 500
		// Note that files are sorted by min key.
		h.put("300", "v300")
		h.put("500", "v500")
		h.compactMem()
		h.put("200", "v200")
		h.put("600", "v600")
		h.put("900", "v900")
		h.compactMem()
		h.tablesPerLevel("2,1,1")

		// Compact away the placeholder files we created initially
		h.compactRangeAt(1, "", "")
		h.compactRangeAt(2, "", "")
		h.tablesPerLevel("2")

		// Do a memtable compaction.  Before bug-fix, the compaction would
		// not detect the overlap with level-0 files and would incorrectly place
		// the deletion in a deeper level.
		h.delete("600")
		h.compactMem()
		h.tablesPerLevel("3")
		h.get("600", false)
	})
}

func TestDB_L0_CompactionBug_Issue44_a(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.reopenDB()
	h.put("b", "v")
	h.reopenDB()
	h.delete("b")
	h.delete("a")
	h.reopenDB()
	h.delete("a")
	h.reopenDB()
	h.put("a", "v")
	h.reopenDB()
	h.reopenDB()
	h.getKeyVal("(a->v)")
	h.waitCompaction()
	h.getKeyVal("(a->v)")
}

func TestDB_L0_CompactionBug_Issue44_b(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.reopenDB()
	h.put("", "")
	h.reopenDB()
	h.delete("e")
	h.put("", "")
	h.reopenDB()
	h.put("c", "cv")
	h.reopenDB()
	h.put("", "")
	h.reopenDB()
	h.put("", "")
	h.waitCompaction()
	h.reopenDB()
	h.put("d", "dv")
	h.reopenDB()
	h.put("", "")
	h.reopenDB()
	h.delete("d")
	h.delete("b")
	h.reopenDB()
	h.getKeyVal("(->)(c->cv)")
	h.waitCompaction()
	h.getKeyVal("(->)(c->cv)")
}

func TestDB_SingleEntryMemCompaction(t *testing.T) {
	trun(t, func(h *dbHarness) {
		for i := 0; i < 10; i++ {
			h.put("big", strings.Repeat("v", opt.DefaultWriteBuffer))
			h.compactMem()
			h.put("key", strings.Repeat("v", opt.DefaultBlockSize))
			h.compactMem()
			h.put("k", "v")
			h.compactMem()
			h.put("", "")
			h.compactMem()
			h.put("verybig", strings.Repeat("v", opt.DefaultWriteBuffer*2))
			h.compactMem()
		}
	})
}

func TestDB_ManifestWriteError(t *testing.T) {
	for i := 0; i < 2; i++ {
		func() {
			h := newDbHarness(t)
			defer h.close()

			h.put("foo", "bar")
			h.getVal("foo", "bar")

			// Mem compaction (will succeed)
			h.compactMem()
			h.getVal("foo", "bar")
			v := h.db.s.version()
			if n := v.tLen(0); n != 1 {
				t.Errorf("invalid total tables, want=1 got=%d", n)
			}
			v.release()

			if i == 0 {
				h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, errors.New("manifest write error"))
			} else {
				h.stor.EmulateError(testutil.ModeSync, storage.TypeManifest, errors.New("manifest sync error"))
			}

			// Merging compaction (will fail)
			h.compactRangeAtErr(0, "", "", true)

			h.db.Close()
			h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, nil)
			h.stor.EmulateError(testutil.ModeSync, storage.TypeManifest, nil)

			// Should not lose data
			h.openDB()
			h.getVal("foo", "bar")
		}()
	}
}

// TestDB_TransactionManifestWriteError ensures a transaction whose commit
// fails to write the manifest releases the compaction commit lock, which a
// compaction under way takes before it stops, and Close waits for it.
func TestDB_TransactionManifestWriteError(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	// Leave the memdb empty so opening a transaction does not commit to
	// the manifest itself.
	h.put("foo", "bar")
	h.compactMem()

	h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, errors.New("manifest write error"))
	tr, err := h.db.OpenTransaction()
	if err != nil {
		t.Fatal("OpenTransaction: got error: ", err)
	}
	if err := tr.Put([]byte("foo"), []byte("baz"), nil); err != nil {
		t.Fatal("Transaction.Put: got error: ", err)
	}
	if err := tr.Commit(); err == nil {
		t.Fatal("Transaction.Commit: expected error")
	}
	tr.Discard()

	// Compactions stop along with writes. One that reaches its commit takes
	// the commit lock before it sees that, so the lock must be free, and
	// manifestSize takes it.
	size := h.manifestSize()
	if err := h.db.CompactRange(util.Range{}); err == nil {
		t.Error("CompactRange: expected error")
	}
	if got := h.manifestSize(); got != size {
		t.Errorf("MANIFEST grew from %d to %d bytes after the failed commit", size, got)
	}

	h.db.Close()
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, nil)

	// Should not lose data
	h.openDB()
	h.getVal("foo", "bar")
}

// TestDB_JournalSyncErrorStopsWrites ensures a write whose journal sync fails
// moves the sequence past it and stops every later write until the DB is
// reopened, as LevelDB does, rather than letting the next write reuse its
// sequence numbers.
func TestDB_JournalSyncErrorStopsWrites(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("a", "1")
	seq := h.db.getSeq()

	h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, errors.New("journal sync error"))
	if err := h.db.Put([]byte("b"), []byte("2"), &opt.WriteOptions{Sync: true}); err == nil {
		t.Fatal("put with a failing journal sync: expected error")
	}
	if got := h.db.getSeq(); got != seq+1 {
		t.Errorf("sequence %d after the failed write, want %d", got, seq+1)
	}
	h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, nil)

	if err := h.db.Put([]byte("c"), []byte("3"), nil); err == nil {
		t.Error("put after a failed journal sync: expected error")
	}
	if tr, err := h.db.OpenTransaction(); err == nil {
		tr.Discard()
		t.Error("transaction after a failed journal sync: expected error")
	}

	h.reopenDB()
	h.put("c", "3")
	h.getVal("a", "1")
	h.getVal("c", "3")
}

// TestDB_JournalWriteErrorSkipsSequence ensures a write whose journal write
// fails moves the sequence past it, so no later write reuses its numbers, and
// stops later writes until the DB is reopened, as a failed sync does: either
// way the journal may end in a torn record.
func TestDB_JournalWriteErrorSkipsSequence(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("a", "1")
	seq := h.db.getSeq()

	h.stor.EmulateError(testutil.ModeWrite, storage.TypeJournal, errors.New("journal write error"))
	if err := h.db.Put([]byte("b"), []byte("2"), nil); err == nil {
		t.Fatal("put with a failing journal write: expected error")
	}
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeJournal, nil)
	if got := h.db.getSeq(); got != seq+1 {
		t.Errorf("sequence %d after the failed write, want %d", got, seq+1)
	}
	if err := h.db.Put([]byte("c"), []byte("3"), nil); !stderrors.Is(err, ErrWritesStopped) {
		t.Errorf("put after a failed journal write = %v, want an ErrWritesStopped match", err)
	}

	h.reopenDB()
	h.getVal("a", "1")
	h.put("c", "3")
}

// TestDB_NoJournalRotationAfterStop ensures a memdb rotation creates no
// journal once writes have stopped, as CompactRange and the write path can
// attempt after their own check for a stop. The rotation went ahead, leaving
// a new journal and a frozen memdb the stopped compactions never flushed.
func TestDB_NoJournalRotationAfterStop(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("a", "1")
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeJournal, errors.New("journal write error"))
	if err := h.db.Put([]byte("b"), []byte("2"), nil); !stderrors.Is(err, ErrWritesStopped) {
		t.Fatalf("put with a failing journal write = %v, want an ErrWritesStopped match", err)
	}
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeJournal, nil)
	before, err := h.stor.List(storage.TypeJournal)
	if err != nil {
		t.Fatal(err)
	}

	// Rotate as CompactRange does once past its check.
	h.db.writeLockC <- struct{}{}
	_, err = h.db.rotateMem(0, false)
	<-h.db.writeLockC
	if !stderrors.Is(err, ErrWritesStopped) {
		t.Errorf("rotation after writes stopped = %v, want an ErrWritesStopped match", err)
	}
	after, err := h.stor.List(storage.TypeJournal)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Errorf("journals after the rotation: %v, want %v", after, before)
	}
}

// TestDB_StopReportedBySetReadOnlyAndClose ensures SetReadOnly and Close
// report that writes have stopped. After a journal failure, SetReadOnly
// returned nil and switched what writes got to ErrReadOnly, and Close
// returned nil after every stop the compaction error goroutine did not hold:
// journal and transaction stops, and a compaction's after SetReadOnly.
func TestDB_StopReportedBySetReadOnlyAndClose(t *testing.T) {
	t.Run("stop before SetReadOnly", func(t *testing.T) {
		h := newDbHarness(t)
		defer h.close()

		h.put("a", "1")
		h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, errors.New("journal sync error"))
		if err := h.db.Put([]byte("b"), []byte("2"), &opt.WriteOptions{Sync: true}); !stderrors.Is(err, ErrWritesStopped) {
			t.Fatalf("put with a failing journal sync = %v, want an ErrWritesStopped match", err)
		}
		h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, nil)

		if err := h.db.SetReadOnly(); !stderrors.Is(err, ErrWritesStopped) {
			t.Errorf("SetReadOnly after writes stopped = %v, want an ErrWritesStopped match", err)
		}
		if err := h.db.Put([]byte("c"), []byte("3"), nil); !stderrors.Is(err, ErrWritesStopped) {
			t.Errorf("put after SetReadOnly = %v, want an ErrWritesStopped match", err)
		}
		if err := h.db.Close(); !stderrors.Is(err, ErrWritesStopped) {
			t.Errorf("Close after writes stopped = %v, want an ErrWritesStopped match", err)
		}
	})

	t.Run("stop after SetReadOnly", func(t *testing.T) {
		h := newDbHarness(t)
		defer h.close()

		h.put("a", "1")
		h.compactMem()
		if err := h.db.SetReadOnly(); err != nil {
			t.Fatal("SetReadOnly: got error: ", err)
		}
		// Compactions carry on in read-only mode; this one's commit fails.
		// The command goes to the compaction goroutine directly: in
		// read-only mode compTriggerRange can return ErrReadOnly before it
		// is sent.
		h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, errors.New("manifest write error"))
		ack := make(chan error, 1)
		h.db.tcompCmdC <- cRange{level: 0, ackC: ack}
		if err := <-ack; !stderrors.Is(err, ErrWritesStopped) {
			t.Fatalf("compaction with a failing MANIFEST write = %v, want an ErrWritesStopped match", err)
		}
		h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, nil)
		if err := h.db.Close(); !stderrors.Is(err, ErrWritesStopped) {
			t.Errorf("Close after a compaction stopped writes = %v, want an ErrWritesStopped match", err)
		}
	})
}

// TestDB_ConcurrentSyncErrorStopsEveryWriter ensures that when journal syncs
// fail, every concurrent writer gets an error matching ErrWritesStopped,
// whether its batch was the one written, was merged into it, or came after
// the stop, and that nothing is written after the stop.
func TestDB_ConcurrentSyncErrorStopsEveryWriter(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, errors.New("journal sync error"))
	const n = 16
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- h.db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"), &opt.WriteOptions{Sync: true})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !stderrors.Is(err, ErrWritesStopped) {
			t.Errorf("concurrent put with failing journal syncs = %v, want an ErrWritesStopped match", err)
		}
	}
	h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, nil)
	if err := h.db.Put([]byte("after"), []byte("v"), nil); !stderrors.Is(err, ErrWritesStopped) {
		t.Errorf("put after the stop = %v, want an ErrWritesStopped match", err)
	}
}

// TestDB_WritesStoppedMatchesSentinel ensures every call that refuses work
// once writes have stopped returns an error matching ErrWritesStopped, so
// callers can tell a stop that lasts until reopen from a passing failure.
func TestDB_WritesStoppedMatchesSentinel(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, errors.New("journal sync error"))
	err := h.db.Put([]byte("a"), []byte("1"), &opt.WriteOptions{Sync: true})
	h.stor.EmulateError(testutil.ModeSync, storage.TypeJournal, nil)
	if !stderrors.Is(err, ErrWritesStopped) {
		t.Errorf("put with a failing journal sync = %v, want an ErrWritesStopped match", err)
	}

	batch := new(Batch)
	batch.Put([]byte("b"), []byte("2"))
	for _, c := range []struct {
		name string
		call func() error
	}{
		{"Put", func() error { return h.db.Put([]byte("b"), []byte("2"), nil) }},
		{"Delete", func() error { return h.db.Delete([]byte("b"), nil) }},
		{"Write", func() error { return h.db.Write(batch, nil) }},
		{"OpenTransaction", func() error {
			tr, err := h.db.OpenTransaction()
			if err == nil {
				tr.Discard()
			}
			return err
		}},
		{"CompactRange", func() error { return h.db.CompactRange(util.Range{}) }},
	} {
		if err := c.call(); !stderrors.Is(err, ErrWritesStopped) {
			t.Errorf("%s after writes stopped = %v, want an ErrWritesStopped match", c.name, err)
		}
	}
}

// TestDB_OpenTransactionErrorReleasesWriteLock ensures a transaction that
// fails to open, here because flushing the memdb before it fails, releases
// the write lock, so later writes and Close do not block on it.
func TestDB_OpenTransactionErrorReleasesWriteLock(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("a", "1")
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeTable, errors.New("table write error"))
	if tr, err := h.db.OpenTransaction(); err == nil {
		tr.Discard()
		t.Fatal("open transaction with a failing memdb flush: expected error")
	}
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeTable, nil)

	// With the write lock still held, these would block for good. Close
	// reports the failed flush, which may not have been retried yet.
	h.put("b", "2")
	h.db.Close()

	h.openDB()
	h.getVal("a", "1")
	h.getVal("b", "2")
}

// TestDB_OpenTransactionWaitsForFlush ensures a transaction opened while a
// memdb flush is still under way waits for the flush, even though the current
// memdb is empty. A transaction that committed first left the flush, which
// records the older sequence at its rotation, to move the MANIFEST's sequence
// back below the transaction's, so writes after a reopen reused its numbers.
func TestDB_OpenTransactionWaitsForFlush(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()

	// A Put larger than the write buffer rotates the memdb right after it,
	// leaving the current memdb empty while the frozen one is flushed. The
	// flush fails, so it is still under way when the transaction opens, and
	// waiting for it returns its error.
	big := bytes.Repeat([]byte{'x'}, 128*opt.KiB)
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeTable, errors.New("table write error"))
	if err := h.db.Put([]byte("big"), big, nil); err != nil {
		t.Fatal("Put: got error: ", err)
	}
	if tr, err := h.db.OpenTransaction(); err == nil {
		tr.Discard()
		t.Fatal("transaction while the memdb flush fails: expected the flush's error")
	}
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeTable, nil)

	// The frozen memdb survives in its journal.
	h.db.Close()
	h.openDB()
	h.getVal("big", string(big))
}

// TestDB_LargeWriteCommitErrorReleasesWriteLock ensures a Write large enough
// to run as a transaction discards it when the commit fails, so the write
// lock is released and later writes and Close do not block on it.
func TestDB_LargeWriteCommitErrorReleasesWriteLock(t *testing.T) {
	for _, test := range []struct {
		name    string
		ft      storage.FileType
		stopped bool // whether the failure stops writes until reopen
	}{
		// The commit's flush fails to write the table.
		{"table", storage.TypeTable, false},
		// The commit fails to write its MANIFEST record.
		{"manifest", storage.TypeManifest, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
			defer h.close()

			// One value over the write buffer, so the batch runs as a
			// transaction that only its commit flushes to a table.
			batch := new(Batch)
			batch.Put([]byte("big"), bytes.Repeat([]byte{'x'}, 128*opt.KiB))
			h.stor.EmulateError(testutil.ModeWrite, test.ft, errors.New("write error"))
			if err := h.db.Write(batch, nil); err == nil {
				t.Fatal("large write with a failing commit: expected error")
			}
			h.stor.EmulateError(testutil.ModeWrite, test.ft, nil)

			// With the write lock still held, the Put and Close would block
			// for good.
			err := h.db.Put([]byte("a"), []byte("1"), nil)
			if test.stopped {
				if !stderrors.Is(err, ErrWritesStopped) {
					t.Errorf("put after a failed MANIFEST commit = %v, want an ErrWritesStopped match", err)
				}
			} else if err != nil {
				t.Error("put after a failed table flush: got error: ", err)
			}
			if _, err := h.db.Get([]byte("big"), nil); err != errors.ErrNotFound {
				t.Errorf("get big = %v, want not found", err)
			}
			h.db.Close()
		})
	}
}

// TestDB_CompactionManifestErrorStopsWrites ensures a compaction whose
// MANIFEST commit fails stops every later write and transaction until the DB
// is reopened, as LevelDB does. The compaction used to retry for good while
// holding the commit lock, so the next transaction waited on the lock, and
// every later write behind it.
func TestDB_CompactionManifestErrorStopsWrites(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()

	h.put("a", "1")
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, errors.New("manifest write error"))
	// Flushes the memdb, whose commit fails.
	if err := h.db.CompactRange(util.Range{}); !isWritesStopped(err) {
		t.Fatalf("CompactRange with a failing MANIFEST = %v, want writes stopped", err)
	}
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeManifest, nil)

	// A Write large enough to run as a transaction, then a Put.
	batch := new(Batch)
	batch.Put([]byte("big"), bytes.Repeat([]byte{'x'}, 128*opt.KiB))
	if err := h.db.Write(batch, nil); !isWritesStopped(err) {
		t.Errorf("large write after a failed MANIFEST commit = %v, want writes stopped", err)
	}
	if err := h.db.Put([]byte("b"), []byte("2"), nil); !isWritesStopped(err) {
		t.Errorf("put after a failed MANIFEST commit = %v, want writes stopped", err)
	}

	// Close reports the stop. The reopened DB replays the journal that the
	// failed flush kept.
	h.db.Close()
	h.openDB()
	h.getVal("a", "1")
	h.put("b", "2")
	h.getVal("b", "2")
}

// manifestSize returns the size of the current MANIFEST.
func (h *dbHarness) manifestSize() int64 {
	h.db.compCommitLk.Lock()
	defer h.db.compCommitLk.Unlock()
	return h.db.s.manifest.Size()
}

// TestDB_TransactionManifestErrorStopsWrites ensures a transaction whose
// MANIFEST commit fails moves the sequence to its own and stops every later
// write and compaction until the DB is reopened, and that discarding it keeps
// its tables, which the MANIFEST may list if the record reached the disk. The
// next open keeps them if it does and removes them if not.
func TestDB_TransactionManifestErrorStopsWrites(t *testing.T) {
	for _, test := range []struct {
		name     string
		mode     testutil.StorageMode
		recorded bool // whether the MANIFEST record reaches the disk
	}{
		{"write", testutil.ModeWrite, false},
		{"sync", testutil.ModeSync, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newDbHarness(t)
			defer h.close()

			// Leave the memdb empty so opening the transaction does not
			// commit to the MANIFEST itself.
			h.put("a", "1")
			h.compactMem()

			h.stor.EmulateError(test.mode, storage.TypeManifest, errors.New("manifest error"))
			tr, err := h.db.OpenTransaction()
			if err != nil {
				t.Fatal("OpenTransaction: got error: ", err)
			}
			if err := tr.Put([]byte("b"), []byte("2"), nil); err != nil {
				t.Fatal("Transaction.Put: got error: ", err)
			}
			if err := tr.Commit(); err == nil {
				t.Fatal("Transaction.Commit: expected error")
			}
			if got := h.db.getSeq(); got != tr.seq {
				t.Errorf("sequence %d after the failed commit, want %d", got, tr.seq)
			}
			if err := tr.Put([]byte("d"), []byte("4"), nil); !stderrors.Is(err, ErrWritesStopped) {
				t.Errorf("transaction put after its failed commit = %v, want an ErrWritesStopped match", err)
			}
			var trTables []storage.FileDesc
			for _, t := range tr.tables {
				trTables = append(trTables, t.fd)
			}
			if len(trTables) == 0 {
				t.Fatal("the transaction made no tables, so the table checks would prove nothing")
			}
			// Listed rather than opened: the DB may hold a table open, and
			// the test storage refuses to open a file twice.
			exists := func(fd storage.FileDesc) bool {
				fds, err := h.stor.List(storage.TypeTable)
				if err != nil {
					t.Fatal("List: got error: ", err)
				}
				for _, x := range fds {
					if x == fd {
						return true
					}
				}
				return false
			}
			tr.Discard()
			for _, fd := range trTables {
				if !exists(fd) {
					t.Errorf("table %v removed by Discard", fd)
				}
			}

			if err := h.db.Put([]byte("c"), []byte("3"), nil); err == nil {
				t.Error("put after a failed MANIFEST commit: expected error")
			}
			if tr, err := h.db.OpenTransaction(); err == nil {
				tr.Discard()
				t.Error("transaction after a failed MANIFEST commit: expected error")
			}

			// Compactions stop too, and append nothing to the MANIFEST,
			// even once it takes writes again.
			h.stor.EmulateError(test.mode, storage.TypeManifest, nil)
			size := h.manifestSize()
			if err := h.db.CompactRange(util.Range{}); !isWritesStopped(err) {
				t.Fatalf("CompactRange after a failed MANIFEST commit = %v, want writes stopped", err)
			}
			if got := h.manifestSize(); got != size {
				t.Errorf("MANIFEST grew from %d to %d bytes after the failed commit", size, got)
			}

			h.db.Close()
			h.openDB()
			h.getVal("a", "1")
			if test.recorded {
				h.getVal("b", "2")
			} else if _, err := h.db.Get([]byte("b"), nil); err != errors.ErrNotFound {
				t.Errorf("get b = %v, want not found", err)
			}
			for _, fd := range trTables {
				if kept := exists(fd); kept != test.recorded {
					t.Errorf("table %v kept %v after reopen, want %v", fd, kept, test.recorded)
				}
			}
		})
	}
}

// pauseTableStorage, once armed, pauses the next table creation until
// released, and reports when it gets there.
type pauseTableStorage struct {
	storage.Storage
	armed   int32
	reached chan struct{}
	release chan struct{}
}

func (s *pauseTableStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	if fd.Type == storage.TypeTable && atomic.CompareAndSwapInt32(&s.armed, 1, 0) {
		close(s.reached)
		<-s.release
	}
	return s.Storage.Create(fd)
}

// TestDB_TransactionRefusedCommitDiscardsTables ensures a transaction whose
// commit the session refuses unwritten, because a compaction's MANIFEST
// commit failed while the transaction flushed its memdb, has its tables
// removed by Discard. Commit kept them whenever the session returned an
// error, as if its record might have reached the MANIFEST: they stayed until
// the next open, and a Recover before then brought the transaction back.
func TestDB_TransactionRefusedCommitDiscardsTables(t *testing.T) {
	mem := storage.NewMemStorage()
	defer mem.Close()
	stor := &pauseTableStorage{Storage: mem, reached: make(chan struct{}), release: make(chan struct{})}
	db, err := Open(stor, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tr, err := db.OpenTransaction()
	if err != nil {
		t.Fatal("OpenTransaction: got error: ", err)
	}
	if err := tr.Put([]byte("k"), []byte("v"), nil); err != nil {
		t.Fatal("Transaction.Put: got error: ", err)
	}

	// Commit checks for a stop, then flushes its memdb to a table. Pause it
	// there, and fail a compaction's MANIFEST commit meanwhile as
	// compactionCommit does, under compCommitLk.
	atomic.StoreInt32(&stor.armed, 1)
	done := make(chan error, 1)
	go func() { done <- tr.Commit() }()
	<-stor.reached
	db.compCommitLk.Lock()
	db.s.manifestErr = errors.New("manifest error")
	_ = db.stopWrites(&writesStoppedError{"MANIFEST commit", db.s.manifestErr})
	db.compCommitLk.Unlock()
	close(stor.release)

	if err := <-done; !stderrors.Is(err, ErrWritesStopped) {
		t.Fatalf("Commit after a compaction stopped writes = %v, want an ErrWritesStopped match", err)
	}
	if len(tr.tables) == 0 {
		t.Fatal("the transaction made no tables, so the table check would prove nothing")
	}
	tr.Discard()
	tables, err := mem.List(storage.TypeTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 0 {
		t.Errorf("tables after Discard: %v, want none: the session never wrote the record", tables)
	}
}

// TestSessionManifestRebuildThrottled ensures a MANIFEST whose snapshot is
// already over MaxManifestFileSize is rebuilt only once it has doubled since
// its last rebuild, not on every commit, and that the grown MANIFEST still
// recovers.
func TestSessionManifestRebuildThrottled(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	o := &opt.Options{MaxManifestFileSize: 1}
	s, err := newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}

	// The size of the snapshot each rebuild starts the MANIFEST with.
	const commits = 50
	base := s.baseManifestSize
	rebuilds := 0
	for i := 1; i <= commits; i++ {
		fd, size := s.manifestFd, s.manifest.Size()
		rec := &sessionRecord{}
		rec.setSeqNum(uint64(i))
		if err := s.commit(rec, false); err != nil {
			t.Fatal(err)
		}
		if s.manifestFd == fd {
			continue
		}
		if size < 2*base {
			t.Fatalf("commit %d rebuilt a %d byte MANIFEST, %d bytes after its last rebuild",
				i, size, base)
		}
		rebuilds++
		base = s.baseManifestSize
	}
	if rebuilds == 0 {
		t.Fatal("the MANIFEST was never rebuilt")
	}
	s.close()
	s.release()

	s, err = newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release()
	defer s.close()
	if err := s.recover(); err != nil {
		t.Fatal(err)
	}
	if s.stSeqNum != commits {
		t.Fatalf("recovered sequence %d, want %d", s.stSeqNum, commits)
	}
}

// TestSessionManifestRotationKeepsRecord ensures the commit that rotates the
// MANIFEST still records its journal number, sequence number and tables, on
// disk and in the session, and does not add the version's other tables to
// its record, which leaked them (syndtr/goleveldb#409).
func TestSessionManifestRotationKeepsRecord(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	o := &opt.Options{MaxManifestFileSize: 1}
	s, err := newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}
	ik := func(ukey string) internalKey {
		return makeInternalKey(nil, []byte(ukey), 1, keyTypeVal)
	}

	// A table already in the version, then enough commits that the next
	// one rotates the MANIFEST.
	rec := &sessionRecord{}
	rec.setSeqNum(1)
	rec.addTable(1, s.allocFileNum(), 1, ik("x"), ik("y"))
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	for s.manifest.Size() < 2*s.baseManifestSize {
		rec := &sessionRecord{}
		rec.setSeqNum(1)
		if err := s.commit(rec, false); err != nil {
			t.Fatal(err)
		}
	}

	fd := s.manifestFd
	rec = &sessionRecord{}
	rec.setJournalNum(7)
	rec.setSeqNum(100)
	rec.addTable(0, s.allocFileNum(), 1, ik("a"), ik("b"))
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	if s.manifestFd == fd {
		t.Fatal("the commit did not rotate the MANIFEST")
	}
	if n := len(rec.addedTables); n != 1 {
		t.Errorf("the rotating commit's record lists %d added tables, want 1", n)
	}
	if s.stJournalNum != 7 || s.stSeqNum != 100 {
		t.Errorf("session journal %d sequence %d after the rotating commit, want 7 and 100",
			s.stJournalNum, s.stSeqNum)
	}
	s.close()
	s.release()

	s, err = newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release()
	defer s.close()
	if err := s.recover(); err != nil {
		t.Fatal(err)
	}
	if s.stJournalNum != 7 || s.stSeqNum != 100 {
		t.Errorf("recovered journal %d sequence %d, want 7 and 100",
			s.stJournalNum, s.stSeqNum)
	}
	v := s.version()
	defer v.release()
	if len(v.levels) < 2 || len(v.levels[0]) != 1 || len(v.levels[1]) != 1 {
		t.Errorf("recovered tables per level %v, want one at level 0 and one at level 1",
			v.levels)
	}
}

// TestSessionManifestSyncErrorStopsCommits ensures that once a MANIFEST sync
// fails, nothing more is appended to the MANIFEST: the failed record may be
// torn on disk, and the next open drops whatever follows a torn record in its
// block, which would lose a later commit that reported success.
func TestSessionManifestSyncErrorStopsCommits(t *testing.T) {
	gomega.RegisterTestingT(t)
	stor := testutil.NewStorage()
	stor.OnLog(testingLogger(t))
	defer stor.Close()
	s, err := newSession(stor, &opt.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.release()
	defer s.close()
	if err := s.create(); err != nil {
		t.Fatal(err)
	}

	stor.EmulateError(testutil.ModeSync, storage.TypeManifest, errors.New("manifest sync error"))
	rec := &sessionRecord{}
	rec.setSeqNum(1)
	if err := s.commit(rec, false); err == nil {
		t.Fatal("commit with a failing MANIFEST sync: expected error")
	}
	stor.EmulateError(testutil.ModeSync, storage.TypeManifest, nil)

	size := s.manifest.Size()
	rec = &sessionRecord{}
	rec.setSeqNum(2)
	if err := s.commit(rec, false); err == nil {
		t.Error("commit after a failed MANIFEST sync: expected error")
	}
	if got := s.manifest.Size(); got != size {
		t.Errorf("MANIFEST grew from %d to %d bytes after the failed sync", size, got)
	}
}

// TestSessionCommitKeepsHighestSequence ensures a commit never lowers the
// sequence the MANIFEST records, as a memdb flush that records the sequence
// at its rotation could after a transaction, and that recovery takes the
// highest sequence in the MANIFEST rather than the last, which a MANIFEST an
// older version wrote can hold lower.
func TestSessionCommitKeepsHighestSequence(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	o := &opt.Options{}
	s, err := newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{10, 5} {
		rec := &sessionRecord{}
		rec.setSeqNum(seq)
		if err := s.commit(rec, false); err != nil {
			t.Fatal(err)
		}
	}
	if s.stSeqNum != 10 {
		t.Errorf("sequence %d after committing 10 then 5, want 10", s.stSeqNum)
	}

	// Append a lower sequence directly, as an older version could have.
	rec := &sessionRecord{}
	rec.setSeqNum(3)
	if err := s.flushManifest(rec); err != nil {
		t.Fatal(err)
	}
	s.close()
	s.release()

	s, err = newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release()
	defer s.close()
	if err := s.recover(); err != nil {
		t.Fatal(err)
	}
	if s.stSeqNum != 10 {
		t.Errorf("recovered sequence %d, want 10", s.stSeqNum)
	}
}

// TestDB_OpenRejectsSequencePastInternalKey ensures an open fails, as corrupt,
// on a MANIFEST whose sequence is past what an internal key holds. The open
// succeeded, and every read then panicked building its lookup key.
func TestDB_OpenRejectsSequencePastInternalKey(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	o := &opt.Options{}
	s, err := newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}
	rec := &sessionRecord{}
	rec.setSeqNum(keyMaxSeq + 1)
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	s.close()
	s.release()

	db, err := Open(stor, o)
	if err == nil {
		db.Close()
		t.Fatal("open with a MANIFEST sequence past keyMaxSeq: expected an error")
	}
	if !errors.IsCorrupted(err) {
		t.Errorf("open with a MANIFEST sequence past keyMaxSeq = %v, want a corruption error", err)
	}
}

// TestSessionFirstCommitListsTablesOnce ensures the first commit after an
// open, which starts a new MANIFEST with a snapshot of the version, lists each
// table once in its record. The record is also the delta the reference loop
// counts, so a table listed twice kept a second reference that no compaction
// dropped, and its file stayed until the next open.
func TestSessionFirstCommitListsTablesOnce(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	o := &opt.Options{}
	s, err := newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}
	ik := func(ukey string) internalKey {
		return makeInternalKey(nil, []byte(ukey), 1, keyTypeVal)
	}
	rec := &sessionRecord{}
	rec.setSeqNum(1)
	rec.addTable(0, s.allocFileNum(), 1, ik("a"), ik("b"))
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	s.close()
	s.release()

	s, err = newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release()
	defer s.close()
	if err := s.recover(); err != nil {
		t.Fatal(err)
	}
	rec = &sessionRecord{}
	rec.setSeqNum(2)
	num := s.allocFileNum()
	rec.addTable(0, num, 1, ik("c"), ik("d"))
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	listed := map[int64]int{}
	for _, at := range rec.addedTables {
		listed[at.num]++
	}
	for n, k := range listed {
		if k != 1 {
			t.Errorf("the first commit after an open lists table %d %d times, want once", n, k)
		}
	}
	if listed[num] != 1 || len(listed) != 2 {
		t.Errorf("the first commit after an open lists tables %v, want the old one and %d", listed, num)
	}
}

// metaErrStorage, once armed, reports an error from SetMeta after it has
// switched CURRENT, as a failed directory sync does.
type metaErrStorage struct {
	storage.Storage
	fail bool
}

func (s *metaErrStorage) SetMeta(fd storage.FileDesc) error {
	if err := s.Storage.SetMeta(fd); err != nil || !s.fail {
		return err
	}
	s.fail = false
	return errors.New("directory sync error")
}

// TestSessionManifestKeptAfterSetMetaError ensures a rotation whose SetMeta
// fails keeps the new MANIFEST, which CURRENT may already name: it is complete
// and synced by then. The cleanup used to remove it, and give its number
// back, so CURRENT could name a file that no longer existed.
func TestSessionManifestKeptAfterSetMetaError(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	ms := &metaErrStorage{Storage: stor}
	o := &opt.Options{MaxManifestFileSize: 1}
	s, err := newSession(ms, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}
	commit := func() error {
		rec := &sessionRecord{}
		rec.setSeqNum(1)
		return s.commit(rec, false)
	}
	for s.manifest.Size() < 2*s.baseManifestSize {
		if err := commit(); err != nil {
			t.Fatal(err)
		}
	}
	ms.fail = true
	if err := commit(); err == nil {
		t.Fatal("rotating commit with a failing SetMeta: expected error")
	}
	s.close()
	s.release()

	s, err = newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release()
	defer s.close()
	if err := s.recover(); err != nil {
		t.Fatalf("recover after a failed SetMeta: %v", err)
	}
}

// TestRecoverManifestListsTablesFirst ensures the MANIFEST that Recover
// writes lists the recovered tables in its first record, since CURRENT names
// the MANIFEST as soon as that record is written. Recover used to write an
// empty snapshot first and the tables in a second record. If that record
// never reached the disk, the next open found no tables and deleted them all.
func TestRecoverManifestListsTablesFirst(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = RecoverFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Cut the MANIFEST back to its first record, as a crash right after
	// Recover wrote it would leave it. A record that fits in one block takes
	// a 7-byte header and its payload.
	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, strings.TrimSpace(string(current)))
	f, err := os.Open(manifest)
	if err != nil {
		t.Fatal(err)
	}
	r, err := journal.NewReader(f, nil, true, true).Next()
	if err != nil {
		t.Fatal(err)
	}
	var first bytes.Buffer
	if _, err := first.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Truncate(manifest, int64(7+first.Len())); err != nil {
		t.Fatal(err)
	}

	db, err = OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, err := db.Get([]byte(key), nil); err != nil {
			t.Errorf("get %s after a crash right after Recover: %v", key, err)
		}
	}
}

// TestRecoverManifestOutnumbersPendingCurrent ensures the MANIFEST Recover
// writes is numbered above every file present, so a pending CURRENT.<n> left
// by a failed SetMeta cannot take precedence at the next open. Recover
// numbered its MANIFEST above the tables only, and the next open preferred a
// leftover CURRENT.<n> naming a higher MANIFEST from before the Recover: it
// then deleted every table written since, or failed on those a compaction
// had replaced.
func TestRecoverManifestOutnumbersPendingCurrent(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Leave what a failed rename in SetMeta leaves: a complete MANIFEST
	// numbered above the tables, and a pending CURRENT.100 naming it.
	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, strings.TrimSpace(string(current))))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MANIFEST-000100"), manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(dir, "CURRENT.100")
	if err := os.WriteFile(pending, []byte("MANIFEST-000100\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Recover, write, and compact, which replaces the table holding "a".
	db, err = RecoverFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("b"), []byte("2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("open after Recover: %v", err)
	}
	defer db.Close()
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}} {
		if v, err := db.Get([]byte(kv[0]), nil); err != nil || string(v) != kv[1] {
			t.Errorf("get %s after Recover = %q, %v; want %s", kv[0], v, err, kv[1])
		}
	}
	if _, err := os.Stat(pending); !os.IsNotExist(err) {
		t.Errorf("CURRENT.100 after the open: %v, want it removed", err)
	}
}

// TestDB_LeftoverJournalRemovedAfterFlush ensures a journal that outlives its
// flush, because removing it failed, is removed after a later flush. Recover
// replays every journal present, so a leftover journal brought back a key
// that was deleted and compacted away after the journal's flush.
func TestDB_LeftoverJournalRemovedAfterFlush(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("k", "v")
	h.stor.EmulateError(testutil.ModeRemove, storage.TypeJournal, errors.New("journal remove error"))
	h.compactMem()
	h.stor.EmulateError(testutil.ModeRemove, storage.TypeJournal, nil)

	// Delete "k", then compact both it and the delete away.
	h.delete("k")
	h.compactMem()
	h.compactRange("", "")
	journals, err := h.stor.List(storage.TypeJournal)
	if err != nil {
		t.Fatal(err)
	}
	if len(journals) != 1 {
		t.Errorf("journals after a later flush: %v, want only the current one", journals)
	}
	h.closeDB()
	h.db, err = Recover(h.stor, h.o)
	if err != nil {
		t.Fatal("Recover: got error: ", err)
	}
	h.get("k", false)
}

// TestDB_ManifestRemoveErrorKeepsWrites ensures failing to remove a MANIFEST
// a newer one replaced stops neither writes nor an open, as in LevelDB: CURRENT
// already names the new MANIFEST, and a later open removes the old one. The
// error used to fail the commit, which stopped writes until the DB was
// reopened, and the open whose first commit replaces the MANIFEST.
func TestDB_ManifestRemoveErrorKeepsWrites(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{MaxManifestFileSize: 1})
	defer h.close()

	manifests := func() []storage.FileDesc {
		fds, err := h.stor.List(storage.TypeManifest)
		if err != nil {
			t.Fatal(err)
		}
		return fds
	}

	// Every flush commits, and past the 1 byte limit the MANIFEST is rebuilt
	// once it has doubled, so a few flushes replace it.
	h.stor.EmulateError(testutil.ModeRemove, storage.TypeManifest, errors.New("manifest remove error"))
	for i := 0; len(manifests()) < 2; i++ {
		if i == 20 {
			t.Fatal("the MANIFEST was never replaced")
		}
		h.put(fmt.Sprintf("k%d", i), "v")
		h.compactMem()
	}
	h.put("after", "v")
	h.compactRange("", "")

	// An open's first commit replaces the MANIFEST it recovered from.
	h.reopenDB()
	h.stor.EmulateError(testutil.ModeRemove, storage.TypeManifest, nil)
	h.reopenDB()
	if fds := manifests(); len(fds) != 1 {
		t.Errorf("MANIFESTs after a reopen: %v, want only the current one", fds)
	}
	h.getVal("k0", "v")
	h.getVal("after", "v")
}

// TestDB_JournalRemoveErrorAtOpen ensures failing to remove the journals an
// open has replayed and flushed does not fail the open, and that a later flush
// removes them. The error used to fail the open, and after the last journal it
// also left the new journal open.
func TestDB_JournalRemoveErrorAtOpen(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB})
	defer h.close()

	// Leave two journals behind: a Put larger than the write buffer rotates
	// the memdb right after it, and the flush of the frozen memdb fails.
	big := bytes.Repeat([]byte{'x'}, 128*opt.KiB)
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeTable, errors.New("table write error"))
	if err := h.db.Put([]byte("big"), big, nil); err != nil {
		t.Fatal("Put: got error: ", err)
	}
	h.put("small", "v")
	h.db.Close()
	h.stor.EmulateError(testutil.ModeWrite, storage.TypeTable, nil)

	// The open replays and flushes both journals, then fails to remove them.
	h.stor.EmulateError(testutil.ModeRemove, storage.TypeJournal, errors.New("journal remove error"))
	h.openDB()
	h.getVal("big", string(big))
	h.getVal("small", "v")

	h.stor.EmulateError(testutil.ModeRemove, storage.TypeJournal, nil)
	h.compactMem()
	journals, err := h.stor.List(storage.TypeJournal)
	if err != nil {
		t.Fatal(err)
	}
	if len(journals) != 1 {
		t.Errorf("journals after a flush: %v, want only the current one", journals)
	}
}

// TestDB_CloseRemovesObsoleteTables ensures Close removes the tables that
// compactions replaced, which the session may not have got to yet, as the
// next open would. Recover reads every table present, so a replaced table left
// behind brought back what the compaction had dropped, deleted keys included.
func TestDB_CloseRemovesObsoleteTables(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("k", "v")
	h.compactMem()
	h.delete("k")
	h.compactMem()
	h.compactRange("", "") // drops both the put and the delete

	h.closeDB()
	tables, err := h.stor.List(storage.TypeTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 0 {
		t.Errorf("tables after Close: %v, want none", tables)
	}

	h.db, err = Recover(h.stor, h.o)
	if err != nil {
		t.Fatal("Recover: got error: ", err)
	}
	h.get("k", false)
}

// closeErrStorage reports an error from closing the first journal it creates,
// after closing it, as close(2) does when a write-back fails.
type closeErrStorage struct {
	storage.Storage
	created int32
}

func (s *closeErrStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	w, err := s.Storage.Create(fd)
	if err == nil && fd.Type == storage.TypeJournal && atomic.AddInt32(&s.created, 1) == 1 {
		return closeErrWriter{w}, nil
	}
	return w, err
}

type closeErrWriter struct{ storage.Writer }

func (w closeErrWriter) Close() error {
	_ = w.Writer.Close()
	return errors.New("journal close error")
}

// TestDB_JournalRotationCloseErrorStopsWrites ensures a rotation that fails to
// close the old journal still switches to the new one, and stops writes until
// the DB is reopened, since the old journal may have lost its last writes, as
// LevelDB does. The rotation used to stop halfway, with writes going to a
// journal the DB did not track, and every later rotation failing again.
func TestDB_JournalRotationCloseErrorStopsWrites(t *testing.T) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	db, err := Open(&closeErrStorage{Storage: stor}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	// Rotates the journal, whose close fails.
	if err := db.CompactRange(util.Range{}); !stderrors.Is(err, ErrWritesStopped) {
		t.Errorf("CompactRange with a failing journal close = %v, want an ErrWritesStopped match", err)
	}
	if err := db.Put([]byte("b"), []byte("2"), nil); !stderrors.Is(err, ErrWritesStopped) {
		t.Errorf("put after a failed journal rotation = %v, want an ErrWritesStopped match", err)
	}
	db.Close()

	db, err = Open(stor, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, err := db.Get([]byte("a"), nil); err != nil || string(v) != "1" {
		t.Errorf("get a after reopen = %q, %v; want 1", v, err)
	}
}

// TestDB_WriteLandsWhenRotationAfterItFails ensures a write that fills the
// memdb succeeds when rotating the memdb after it fails: by then it is in
// the journal and the memdb. It used to report the rotation's error, for it
// and every write merged with it, though they had all landed. A later write
// that needs room retries the rotation before writing anything.
func TestDB_WriteLandsWhenRotationAfterItFails(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, DisableLargeBatchTransaction: true})
	defer h.close()

	// A Put larger than the write buffer fills the memdb, so the memdb
	// rotates right after it, and creating the new journal fails.
	big := strings.Repeat("x", 128*opt.KiB)
	h.stor.EmulateError(testutil.ModeCreate, storage.TypeJournal, errors.New("journal create error"))
	if err := h.db.Put([]byte("big"), []byte(big), nil); err != nil {
		t.Errorf("put whose rotation after it fails = %v, want nil: it landed", err)
	}
	h.getVal("big", big)
	h.stor.EmulateError(testutil.ModeCreate, storage.TypeJournal, nil)

	h.put("a", "1")
	h.reopenDB()
	h.getVal("big", big)
	h.getVal("a", "1")
}

// TestDB_NewRawIteratorAfterClose ensures an iterator built once Close has
// run, as NewIterator racing Close can, after its own check, reports
// ErrClosed. Building it used the memdb that Close had cleared, and panicked.
func TestDB_NewRawIteratorAfterClose(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()
	h.put("a", "1")
	h.closeDB()

	iter := h.db.newRawIterator(nil, nil, nil, nil)
	defer iter.Release()
	if iter.Next() || iter.Error() != ErrClosed {
		t.Errorf("raw iterator after Close: error %v, want %v", iter.Error(), ErrClosed)
	}
}

// closerFunc is an io.Closer that calls itself.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// TestDB_NewRawIteratorDuringClose ensures an iterator built while Close runs,
// once it has closed the session but before it clears the memdbs, reports
// ErrClosed. The session's last version lists no tables, so the iterator saw
// only the memdbs and reported no error, silently missing every key held in a
// table.
func TestDB_NewRawIteratorDuringClose(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()
	h.put("b", "2")
	h.compactMem()  // "b" is only in a table
	h.put("a", "1") // "a" is only in the memdb

	// Close calls the closer between closing the session and clearing the
	// memdbs, where a NewIterator racing it, past its own check, can build
	// its iterator.
	var keys int
	var iterErr error
	h.db.closer = closerFunc(func() error {
		iter := h.db.newRawIterator(nil, nil, nil, nil)
		for iter.Next() {
			keys++
		}
		iterErr = iter.Error()
		iter.Release()
		return nil
	})
	h.closeDB()
	if keys != 0 || iterErr != ErrClosed {
		t.Errorf("raw iterator during Close: %d keys, error %v; want none, %v", keys, iterErr, ErrClosed)
	}
}

// TestDB_WriteFailsPastMaxSequence ensures a write whose sequence numbers
// would pass what an internal key holds fails before it reaches the journal.
// Recovery moves the sequence one past the last batch and rejects a batch
// that leaves no room for that, so a write that took the very last number
// was dropped at the next open, and the write after that panicked once
// journaled, at every reopen.
func TestDB_WriteFailsPastMaxSequence(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.db.setSeq(keyMaxSeq - 2)
	h.put("a", "1")
	if err := h.db.Put([]byte("b"), []byte("2"), nil); err != errSeqExhausted {
		t.Errorf("put past the maximum sequence = %v, want %v", err, errSeqExhausted)
	}
	tr, err := h.db.OpenTransaction()
	if err != nil {
		t.Fatal("OpenTransaction: got error: ", err)
	}
	if err := tr.Put([]byte("c"), []byte("3"), nil); err != errSeqExhausted {
		t.Errorf("transaction put past the maximum sequence = %v, want %v", err, errSeqExhausted)
	}
	tr.Discard()

	h.reopenDB()
	h.getVal("a", "1")
	h.get("b", false)
	if err := h.db.Put([]byte("b"), []byte("2"), nil); err != errSeqExhausted {
		t.Errorf("put past the maximum sequence after reopen = %v, want %v", err, errSeqExhausted)
	}

	// Nor may a sequence far past the limit, which recovery refuses, wrap
	// the check around.
	h.db.setSeq(^uint64(0))
	if err := h.db.Put([]byte("b"), []byte("2"), nil); err != errSeqExhausted {
		t.Errorf("put with the sequence at 2^64-1 = %v, want %v", err, errSeqExhausted)
	}
}

// TestDB_TransactionWriteFailsWholePastMaxSequence ensures a transaction's
// Write of a batch whose sequence numbers would pass the last usable one fails
// whole, as a DB write does, and that a transaction put cannot wrap the check
// around. Write applied the records that still fitted, and the transaction
// stayed open with half the batch.
func TestDB_TransactionWriteFailsWholePastMaxSequence(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.db.setSeq(keyMaxSeq - 3)
	tr, err := h.db.OpenTransaction()
	if err != nil {
		t.Fatal("OpenTransaction: got error: ", err)
	}
	defer tr.Discard()
	batch := func(keys ...string) *Batch {
		b := new(Batch)
		for _, k := range keys {
			b.Put([]byte(k), []byte("v"))
		}
		return b
	}

	if err := tr.Write(batch("a", "b", "c", "d", "e"), nil); err != errSeqExhausted {
		t.Errorf("transaction write past the maximum sequence = %v, want %v", err, errSeqExhausted)
	}
	if _, err := tr.Get([]byte("a"), nil); err != ErrNotFound {
		t.Errorf("get a after the refused write = %v, want not found: none of the batch may apply", err)
	}
	// The last usable sequence number is keyMaxSeq-1, so two records fit.
	if err := tr.Write(batch("f", "g"), nil); err != nil {
		t.Errorf("transaction write up to the maximum sequence = %v, want nil", err)
	}

	tr.seq = ^uint64(0)
	if err := tr.Put([]byte("h"), []byte("v"), nil); err != errSeqExhausted {
		t.Errorf("transaction put with the sequence at 2^64-1 = %v, want %v", err, errSeqExhausted)
	}
}

// TestDB_NoStaleBlocksAfterDiscardedTransaction ensures a table created after
// a transaction is discarded does not read the discarded tables' cached
// blocks. The block cache keys blocks by file number and offset, and removing
// a table handed its number to the next table without evicting its blocks, so
// that table read the discarded values. With BlockCacheEvictRemoved the
// number is still reused, once the blocks are evicted.
func TestDB_NoStaleBlocksAfterDiscardedTransaction(t *testing.T) {
	for _, evictRemoved := range []bool{false, true} {
		t.Run(fmt.Sprintf("BlockCacheEvictRemoved=%v", evictRemoved), func(t *testing.T) {
			h := newDbHarnessWopt(t, &opt.Options{WriteBuffer: 64 * opt.KiB, BlockCacheEvictRemoved: evictRemoved})
			defer h.close()

			const n = 100
			key := func(i int) []byte { return []byte(fmt.Sprintf("k%03d", i)) }
			value := func(v string) []byte { return []byte(v + strings.Repeat("x", opt.KiB)) }
			write := func(tr *Transaction, v string) {
				for i := 0; i < n; i++ {
					if err := tr.Put(key(i), value(v), nil); err != nil {
						t.Fatal("transaction put: got error: ", err)
					}
				}
			}

			// The discarded transaction flushes a table and reads it back,
			// which puts the table's blocks in the cache.
			tr, err := h.db.OpenTransaction()
			if err != nil {
				t.Fatal("OpenTransaction: got error: ", err)
			}
			write(tr, "old")
			if len(tr.tables) == 0 {
				t.Fatal("the discarded transaction flushed no table")
			}
			if _, err := tr.Get(key(0), nil); err != nil {
				t.Fatal("transaction get: got error: ", err)
			}
			tr.Discard()

			tr, err = h.db.OpenTransaction()
			if err != nil {
				t.Fatal("OpenTransaction: got error: ", err)
			}
			write(tr, "new")
			if err := tr.Commit(); err != nil {
				t.Fatal("Commit: got error: ", err)
			}

			for i := 0; i < n; i++ {
				if v, err := h.db.Get(key(i), nil); err != nil || !bytes.Equal(v, value("new")) {
					t.Fatalf("get %s after the discarded transaction = %.3q, %v; want new", key(i), v, err)
				}
			}
		})
	}
}

func assertErr(t *testing.T, err error, wanterr bool) {
	if err != nil {
		if wanterr {
			t.Log("AssertErr: got error (expected): ", err)
		} else {
			t.Error("AssertErr: got error: ", err)
		}
	} else if wanterr {
		t.Error("AssertErr: expect error")
	}
}

func TestDB_ClosedIsClosed(t *testing.T) {
	h := newDbHarness(t)
	db := h.db

	var iter, iter2 iterator.Iterator
	var snap *Snapshot
	func() {
		defer h.close()

		h.put("k", "v")
		h.getVal("k", "v")

		iter = db.NewIterator(nil, h.ro)
		iter.Seek([]byte("k"))
		testKeyVal(t, iter, "k->v")

		var err error
		snap, err = db.GetSnapshot()
		if err != nil {
			t.Fatal("GetSnapshot: got error: ", err)
		}

		h.getValr(snap, "k", "v")

		iter2 = snap.NewIterator(nil, h.ro)
		iter2.Seek([]byte("k"))
		testKeyVal(t, iter2, "k->v")

		h.put("foo", "v2")
		h.delete("foo")

		// closing DB
		iter.Release()
		iter2.Release()
	}()

	assertErr(t, db.Put([]byte("x"), []byte("y"), h.wo), true)
	_, err := db.Get([]byte("k"), h.ro)
	assertErr(t, err, true)

	if iter.Valid() {
		t.Errorf("iter.Valid should false")
	}
	assertErr(t, iter.Error(), false)
	testKeyVal(t, iter, "->")
	if iter.Seek([]byte("k")) {
		t.Errorf("iter.Seek should false")
	}
	assertErr(t, iter.Error(), true)

	assertErr(t, iter2.Error(), false)

	_, err = snap.Get([]byte("k"), h.ro)
	assertErr(t, err, true)

	_, err = db.GetSnapshot()
	assertErr(t, err, true)

	iter3 := db.NewIterator(nil, h.ro)
	assertErr(t, iter3.Error(), true)

	iter3 = snap.NewIterator(nil, h.ro)
	assertErr(t, iter3.Error(), true)

	assertErr(t, db.Delete([]byte("k"), h.wo), true)

	_, err = db.GetProperty("leveldb.stats")
	assertErr(t, err, true)

	_, err = db.SizeOf([]util.Range{{Start: []byte("a"), Limit: []byte("z")}})
	assertErr(t, err, true)

	assertErr(t, db.CompactRange(util.Range{}), true)

	assertErr(t, db.Close(), true)
}

type numberComparer struct{}

func (numberComparer) num(x []byte) (n int) {
	// A key that does not parse compares as 0.
	_, _ = fmt.Sscan(string(x[1:len(x)-1]), &n)
	return
}

func (numberComparer) Name() string {
	return "test.NumberComparer"
}

func (p numberComparer) Compare(a, b []byte) int {
	return p.num(a) - p.num(b)
}

func (numberComparer) Separator(dst, a, b []byte) []byte { return nil }
func (numberComparer) Successor(dst, b []byte) []byte    { return nil }

func TestDB_CustomComparer(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Comparer:                     numberComparer{},
		WriteBuffer:                  1000,
	})
	defer h.close()

	h.put("[10]", "ten")
	h.put("[0x14]", "twenty")
	for i := 0; i < 2; i++ {
		h.getVal("[10]", "ten")
		h.getVal("[0xa]", "ten")
		h.getVal("[20]", "twenty")
		h.getVal("[0x14]", "twenty")
		h.get("[15]", false)
		h.get("[0xf]", false)
		h.compactMem()
		h.compactRange("[0]", "[9999]")
	}

	for n := 0; n < 2; n++ {
		for i := 0; i < 100; i++ {
			v := fmt.Sprintf("[%d]", i*10)
			h.put(v, v)
		}
		h.compactMem()
		h.compactRange("[0]", "[1000000]")
	}
}

func TestDB_ManualCompaction(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.db.memdbMaxLevel = 2

	h.putMulti(3, "p", "q")
	h.tablesPerLevel("1,1,1")

	// Compaction range falls before files
	h.compactRange("", "c")
	h.tablesPerLevel("1,1,1")

	// Compaction range falls after files
	h.compactRange("r", "z")
	h.tablesPerLevel("1,1,1")

	// Compaction range overlaps files
	h.compactRange("p1", "p9")
	h.tablesPerLevel("0,0,1")

	// Populate a different range
	h.putMulti(3, "c", "e")
	h.tablesPerLevel("1,1,2")

	// Compact just the new range
	h.compactRange("b", "f")
	h.tablesPerLevel("0,0,2")

	// Compact all
	h.putMulti(1, "a", "z")
	h.tablesPerLevel("0,1,2")
	h.compactRange("", "")
	h.tablesPerLevel("0,0,1")
}

func TestDB_BloomFilter(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		DisableBlockCache:            true,
		Filter:                       filter.NewBloomFilter(10),
	})
	defer h.close()

	key := func(i int) string {
		return fmt.Sprintf("key%06d", i)
	}

	const n = 10000

	// Populate multiple layers
	for i := 0; i < n; i++ {
		h.put(key(i), key(i))
	}
	h.compactMem()
	h.compactRange("a", "z")
	for i := 0; i < n; i += 100 {
		h.put(key(i), key(i))
	}
	h.compactMem()

	// Prevent auto compactions triggered by seeks
	h.stor.Stall(testutil.ModeSync, storage.TypeTable)

	// Lookup present keys. Should rarely read from small sstable.
	h.stor.ResetCounter(testutil.ModeRead, storage.TypeTable)
	for i := 0; i < n; i++ {
		h.getVal(key(i), key(i))
	}
	cnt, _ := h.stor.Counter(testutil.ModeRead, storage.TypeTable)
	t.Logf("lookup of %d present keys yield %d sstable I/O reads", n, cnt)
	if min, max := n, n+2*n/100; cnt < min || cnt > max {
		t.Errorf("num of sstable I/O reads of present keys not in range of %d - %d, got %d", min, max, cnt)
	}

	// Lookup missing keys. Should rarely read from either sstable.
	h.stor.ResetCounter(testutil.ModeRead, storage.TypeTable)
	for i := 0; i < n; i++ {
		h.get(key(i)+".missing", false)
	}
	cnt, _ = h.stor.Counter(testutil.ModeRead, storage.TypeTable)
	t.Logf("lookup of %d missing keys yield %d sstable I/O reads", n, cnt)
	if max := 3 * n / 100; cnt > max {
		t.Errorf("num of sstable I/O reads of missing keys was more than %d, got %d", max, cnt)
	}

	h.stor.Release(testutil.ModeSync, storage.TypeTable)
}

func TestDB_Concurrent(t *testing.T) {
	const n, secs, maxkey = 4, 6, 1000
	h := newDbHarness(t)
	defer h.close()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		closeWg sync.WaitGroup
		stop    uint32
		cnt     [n]uint32
	)

	for i := 0; i < n; i++ {
		closeWg.Add(1)
		go func(i int) {
			var put, get, found uint
			defer func() {
				t.Logf("goroutine %d stopped after %d ops, put=%d get=%d found=%d missing=%d",
					i, cnt[i], put, get, found, get-found)
				closeWg.Done()
			}()

			rnd := rand.New(rand.NewSource(int64(1000 + i)))
			for atomic.LoadUint32(&stop) == 0 {
				x := cnt[i]

				k := rnd.Intn(maxkey)
				kstr := fmt.Sprintf("%016d", k)

				if (rnd.Int() % 2) > 0 {
					put++
					h.put(kstr, fmt.Sprintf("%d.%d.%-1000d", k, i, x))
				} else {
					get++
					v, err := h.db.Get([]byte(kstr), h.ro)
					if err == nil {
						found++
						rk, ri, rx := 0, -1, uint32(0)
						if _, err := fmt.Sscanf(string(v), "%d.%d.%d", &rk, &ri, &rx); err != nil {
							t.Errorf("invalid value for key %d: %v", k, err)
						}
						if rk != k {
							t.Errorf("invalid key want=%d got=%d", k, rk)
						}
						if ri < 0 || ri >= n {
							t.Error("invalid goroutine number: ", ri)
						} else {
							tx := atomic.LoadUint32(&(cnt[ri]))
							if rx > tx {
								t.Errorf("invalid seq number, %d > %d ", rx, tx)
							}
						}
					} else if err != ErrNotFound {
						t.Error("Get: got error: ", err)
						return
					}
				}
				atomic.AddUint32(&cnt[i], 1)
			}
		}(i)
	}

	time.Sleep(secs * time.Second)
	atomic.StoreUint32(&stop, 1)
	closeWg.Wait()
}

func TestDB_ConcurrentIterator(t *testing.T) {
	const n, n2 = 4, 1000
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 30})
	defer h.close()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		closeWg sync.WaitGroup
		stop    uint32
	)

	for i := 0; i < n; i++ {
		closeWg.Add(1)
		go func(i int) {
			for k := 0; atomic.LoadUint32(&stop) == 0; k++ {
				h.put(fmt.Sprintf("k%d", k), fmt.Sprintf("%d.%d.", k, i)+strings.Repeat("x", 10))
			}
			closeWg.Done()
		}(i)
	}

	for i := 0; i < n; i++ {
		closeWg.Add(1)
		go func(i int) {
			for k := 1000000; k < 0 || atomic.LoadUint32(&stop) == 0; k-- {
				h.put(fmt.Sprintf("k%d", k), fmt.Sprintf("%d.%d.", k, i)+strings.Repeat("x", 10))
			}
			closeWg.Done()
		}(i)
	}

	cmp := comparer.DefaultComparer
	for i := 0; i < n2; i++ {
		closeWg.Add(1)
		go func(i int) {
			it := h.db.NewIterator(nil, nil)
			var pk []byte
			for it.Next() {
				kk := it.Key()
				if cmp.Compare(kk, pk) <= 0 {
					t.Errorf("iter %d: %q is successor of %q", i, pk, kk)
				}
				pk = append(pk[:0], kk...)
				var k, vk, vi int
				if n, err := fmt.Sscanf(string(it.Key()), "k%d", &k); err != nil {
					t.Errorf("iter %d: Scanf error on key %q: %v", i, it.Key(), err)
				} else if n < 1 {
					t.Errorf("iter %d: Cannot parse key %q", i, it.Key())
				}
				if n, err := fmt.Sscanf(string(it.Value()), "%d.%d", &vk, &vi); err != nil {
					t.Errorf("iter %d: Scanf error on value %q: %v", i, it.Value(), err)
				} else if n < 2 {
					t.Errorf("iter %d: Cannot parse value %q", i, it.Value())
				}

				if vk != k {
					t.Errorf("iter %d: invalid value i=%d, want=%d got=%d", i, vi, k, vk)
				}
			}
			if err := it.Error(); err != nil {
				t.Errorf("iter %d: Got error: %v", i, err)
			}
			it.Release()
			closeWg.Done()
		}(i)
	}

	atomic.StoreUint32(&stop, 1)
	closeWg.Wait()
}

func TestDB_ConcurrentWrite(t *testing.T) {
	const n, bk, niter = 10, 3, 10000
	h := newDbHarness(t)
	defer h.close()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for k := 0; k < niter; k++ {
				kstr := fmt.Sprintf("put-%d.%d", i, k)
				vstr := fmt.Sprintf("v%d", k)
				h.put(kstr, vstr)
				// Key should immediately available after put returns.
				h.getVal(kstr, vstr)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		batch := &Batch{}
		go func(i int) {
			defer wg.Done()
			for k := 0; k < niter; k++ {
				batch.Reset()
				for j := 0; j < bk; j++ {
					batch.Put([]byte(fmt.Sprintf("batch-%d.%d.%d", i, k, j)), []byte(fmt.Sprintf("v%d", k)))
				}
				h.write(batch)
				// Key should immediately available after put returns.
				for j := 0; j < bk; j++ {
					h.getVal(fmt.Sprintf("batch-%d.%d.%d", i, k, j), fmt.Sprintf("v%d", k))
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestDB_CreateReopenDbOnFile(t *testing.T) {
	dbpath := filepath.Join(os.TempDir(), fmt.Sprintf("goleveldbtestCreateReopenDbOnFile-%d", os.Getuid()))
	if err := os.RemoveAll(dbpath); err != nil {
		t.Fatal("cannot remove old db: ", err)
	}
	defer os.RemoveAll(dbpath)

	for i := 0; i < 3; i++ {
		stor, err := storage.OpenFile(dbpath, false)
		if err != nil {
			t.Fatalf("(%d) cannot open storage: %s", i, err)
		}
		db, err := Open(stor, nil)
		if err != nil {
			t.Fatalf("(%d) cannot open db: %s", i, err)
		}
		if err := db.Put([]byte("foo"), []byte("bar"), nil); err != nil {
			t.Fatalf("(%d) cannot write to db: %s", i, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("(%d) cannot close db: %s", i, err)
		}
		if err := stor.Close(); err != nil {
			t.Fatalf("(%d) cannot close storage: %s", i, err)
		}
	}
}

func TestDB_CreateReopenDbOnFile2(t *testing.T) {
	dbpath := filepath.Join(os.TempDir(), fmt.Sprintf("goleveldbtestCreateReopenDbOnFile2-%d", os.Getuid()))
	if err := os.RemoveAll(dbpath); err != nil {
		t.Fatal("cannot remove old db: ", err)
	}
	defer os.RemoveAll(dbpath)

	for i := 0; i < 3; i++ {
		db, err := OpenFile(dbpath, nil)
		if err != nil {
			t.Fatalf("(%d) cannot open db: %s", i, err)
		}
		if err := db.Put([]byte("foo"), []byte("bar"), nil); err != nil {
			t.Fatalf("(%d) cannot write to db: %s", i, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("(%d) cannot close db: %s", i, err)
		}
	}
}

func TestDB_DeletionMarkersOnMemdb(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("foo", "v1")
	h.compactMem()
	h.delete("foo")
	h.get("foo", false)
	h.getKeyVal("")
}

func TestDB_LeveldbIssue178(t *testing.T) {
	nKeys := (opt.DefaultCompactionTableSize / 30) * 5
	key1 := func(i int) string {
		return fmt.Sprintf("my_key_%d", i)
	}
	key2 := func(i int) string {
		return fmt.Sprintf("my_key_%d_xxx", i)
	}

	// Disable compression since it affects the creation of layers and the
	// code below is trying to test against a very specific scenario.
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
	})
	defer h.close()

	// Create first key range.
	batch := new(Batch)
	for i := 0; i < nKeys; i++ {
		batch.Put([]byte(key1(i)), []byte("value for range 1 key"))
	}
	h.write(batch)

	// Create second key range.
	batch.Reset()
	for i := 0; i < nKeys; i++ {
		batch.Put([]byte(key2(i)), []byte("value for range 2 key"))
	}
	h.write(batch)

	// Delete second key range.
	batch.Reset()
	for i := 0; i < nKeys; i++ {
		batch.Delete([]byte(key2(i)))
	}
	h.write(batch)
	h.waitMemCompaction()

	// Run manual compaction.
	h.compactRange(key1(0), key1(nKeys-1))

	// Checking the keys.
	h.assertNumKeys(nKeys)
}

func TestDB_LeveldbIssue200(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("1", "b")
	h.put("2", "c")
	h.put("3", "d")
	h.put("4", "e")
	h.put("5", "f")

	iter := h.db.NewIterator(nil, h.ro)

	// Add an element that should not be reflected in the iterator.
	h.put("25", "cd")

	iter.Seek([]byte("5"))
	assertBytes(t, []byte("5"), iter.Key())
	iter.Prev()
	assertBytes(t, []byte("4"), iter.Key())
	iter.Prev()
	assertBytes(t, []byte("3"), iter.Key())
	iter.Next()
	assertBytes(t, []byte("4"), iter.Key())
	iter.Next()
	assertBytes(t, []byte("5"), iter.Key())
}

func TestDB_GoleveldbIssue74(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  1 * opt.MiB,
	})
	defer h.close()

	const n, dur = 10000, 5 * time.Second

	runtime.GOMAXPROCS(runtime.NumCPU())

	until := time.Now().Add(dur)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	var done uint32
	go func() {
		var i int
		defer func() {
			t.Logf("WRITER DONE #%d", i)
			atomic.StoreUint32(&done, 1)
			wg.Done()
		}()

		b := new(Batch)
		for ; time.Now().Before(until) && atomic.LoadUint32(&done) == 0; i++ {
			if t.Failed() {
				return
			}

			iv := fmt.Sprintf("VAL%010d", i)
			for k := 0; k < n; k++ {
				key := fmt.Sprintf("KEY%06d", k)
				b.Put([]byte(key), []byte(key+iv))
				b.Put([]byte(fmt.Sprintf("PTR%06d", k)), []byte(key))
			}
			h.write(b)

			b.Reset()
			snap := h.getSnapshot()
			iter := snap.NewIterator(util.BytesPrefix([]byte("PTR")), nil)
			var k int
			for ; iter.Next(); k++ {
				if t.Failed() {
					return
				}

				ptrKey := iter.Key()
				key := iter.Value()

				if _, err := snap.Get(ptrKey, nil); err != nil {
					t.Errorf("WRITER #%d snapshot.Get %q: %v", i, ptrKey, err)
					return
				}
				if value, err := snap.Get(key, nil); err != nil {
					t.Errorf("WRITER #%d snapshot.Get %q: %v", i, key, err)
					return
				} else if string(value) != string(key)+iv {
					t.Errorf("WRITER #%d snapshot.Get %q got invalid value, want %q got %q", i, key, string(key)+iv, value)
					return
				}

				b.Delete(key)
				b.Delete(ptrKey)
			}
			h.write(b)
			iter.Release()
			snap.Release()
			if k != n {
				t.Errorf("#%d %d != %d", i, k, n)
			}
		}
	}()
	go func() {
		var i int
		defer func() {
			t.Logf("READER DONE #%d", i)
			atomic.StoreUint32(&done, 1)
			wg.Done()
		}()
		for ; time.Now().Before(until) && atomic.LoadUint32(&done) == 0; i++ {
			if t.Failed() {
				return
			}

			snap := h.getSnapshot()
			iter := snap.NewIterator(util.BytesPrefix([]byte("PTR")), nil)
			var prevValue string
			var k int
			for ; iter.Next(); k++ {
				if t.Failed() {
					return
				}

				ptrKey := iter.Key()
				key := iter.Value()

				if _, err := snap.Get(ptrKey, nil); err != nil {
					t.Errorf("READER #%d snapshot.Get %q: %v", i, ptrKey, err)
					return
				}

				if value, err := snap.Get(key, nil); err != nil {
					t.Errorf("READER #%d snapshot.Get %q: %v", i, key, err)
					return
				} else if prevValue != "" && string(value) != string(key)+prevValue {
					t.Errorf("READER #%d snapshot.Get %q got invalid value, want %q got %q", i, key, string(key)+prevValue, value)
					return
				} else {
					prevValue = string(value[len(key):])
				}
			}
			iter.Release()
			snap.Release()
			if k > 0 && k != n {
				t.Errorf("#%d %d != %d", i, k, n)
			}
		}
	}()
	wg.Wait()
}

func TestDB_GetProperties(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	_, err := h.db.GetProperty("leveldb.num-files-at-level")
	if err == nil {
		t.Error("GetProperty() failed to detect missing level")
	}

	_, err = h.db.GetProperty("leveldb.num-files-at-level0")
	if err != nil {
		t.Error("got unexpected error", err)
	}

	_, err = h.db.GetProperty("leveldb.num-files-at-level0x")
	if err == nil {
		t.Error("GetProperty() failed to detect invalid level")
	}
}

func TestDB_GoleveldbIssue72and83(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  1 * opt.MiB,
		OpenFilesCacheCapacity:       3,
	})
	defer h.close()

	const n, wn, dur = 10000, 100, 30 * time.Second

	runtime.GOMAXPROCS(runtime.NumCPU())

	randomData := func(prefix byte, i int) []byte {
		data := make([]byte, 1+4+32+64+32)
		_, err := crand.Reader.Read(data[1 : len(data)-8])
		if err != nil {
			panic(err)
		}
		data[0] = prefix
		binary.LittleEndian.PutUint32(data[len(data)-8:], uint32(i))
		binary.LittleEndian.PutUint32(data[len(data)-4:], util.NewCRC(data[:len(data)-4]).Value())
		return data
	}

	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = randomData(1, 0)
	}

	until := time.Now().Add(dur)
	wg := new(sync.WaitGroup)
	wg.Add(3)
	var done uint32
	go func() {
		i := 0
		defer func() {
			t.Logf("WRITER DONE #%d", i)
			wg.Done()
		}()

		b := new(Batch)
		for ; i < wn && atomic.LoadUint32(&done) == 0; i++ {
			if t.Failed() {
				return
			}

			b.Reset()
			for _, k1 := range keys {
				k2 := randomData(2, i)
				b.Put(k2, randomData(42, i))
				b.Put(k1, k2)
			}
			if err := h.db.Write(b, h.wo); err != nil {
				atomic.StoreUint32(&done, 1)
				t.Errorf("WRITER #%d db.Write: %v", i, err)
				return
			}
		}
	}()
	go func() {
		var i int
		defer func() {
			t.Logf("READER0 DONE #%d", i)
			atomic.StoreUint32(&done, 1)
			wg.Done()
		}()
		for ; time.Now().Before(until) && atomic.LoadUint32(&done) == 0; i++ {
			if t.Failed() {
				return
			}

			snap := h.getSnapshot()
			seq := snap.elem.seq
			if seq == 0 {
				snap.Release()
				continue
			}
			iter := snap.NewIterator(util.BytesPrefix([]byte{1}), nil)
			writei := int(seq/(n*2) - 1)
			var k int
			for ; iter.Next(); k++ {
				if t.Failed() {
					return
				}

				k1 := iter.Key()
				k2 := iter.Value()
				k1checksum0 := binary.LittleEndian.Uint32(k1[len(k1)-4:])
				k1checksum1 := util.NewCRC(k1[:len(k1)-4]).Value()
				if k1checksum0 != k1checksum1 {
					t.Errorf("READER0 #%d.%d W#%d invalid K1 checksum: %#x != %#x", i, k, writei, k1checksum0, k1checksum0)
					return
				}
				k2checksum0 := binary.LittleEndian.Uint32(k2[len(k2)-4:])
				k2checksum1 := util.NewCRC(k2[:len(k2)-4]).Value()
				if k2checksum0 != k2checksum1 {
					t.Errorf("READER0 #%d.%d W#%d invalid K2 checksum: %#x != %#x", i, k, writei, k2checksum0, k2checksum1)
					return
				}
				kwritei := int(binary.LittleEndian.Uint32(k2[len(k2)-8:]))
				if writei != kwritei {
					t.Errorf("READER0 #%d.%d W#%d invalid write iteration num: %d", i, k, writei, kwritei)
					return
				}
				if _, err := snap.Get(k2, nil); err != nil {
					t.Errorf("READER0 #%d.%d W#%d snap.Get: %v\nk1: %x\n -> k2: %x", i, k, writei, err, k1, k2)
					return
				}
			}
			if err := iter.Error(); err != nil {
				t.Errorf("READER0 #%d.%d W#%d snap.Iterator: %v", i, k, writei, err)
				return
			}
			iter.Release()
			snap.Release()
			if k > 0 && k != n {
				t.Errorf("READER0 #%d W#%d short read, got=%d want=%d", i, writei, k, n)
			}
		}
	}()
	go func() {
		var i int
		defer func() {
			t.Logf("READER1 DONE #%d", i)
			atomic.StoreUint32(&done, 1)
			wg.Done()
		}()
		for ; time.Now().Before(until) && atomic.LoadUint32(&done) == 0; i++ {
			if t.Failed() {
				return
			}

			iter := h.db.NewIterator(nil, nil)
			seq := iter.(*dbIter).seq
			if seq == 0 {
				iter.Release()
				continue
			}
			writei := int(seq/(n*2) - 1)
			var k int
			for ok := iter.Last(); ok; ok = iter.Prev() {
				k++
			}
			if err := iter.Error(); err != nil {
				t.Errorf("READER1 #%d.%d W#%d db.Iterator: %v", i, k, writei, err)
				return
			}
			iter.Release()
			if m := (writei+1)*n + n; k != m {
				t.Errorf("READER1 #%d W#%d short read, got=%d want=%d", i, writei, k, m)
				return
			}
		}
	}()

	wg.Wait()
}

func TestDB_TransientError(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  128 * opt.KiB,
		OpenFilesCacheCapacity:       3,
		DisableCompactionBackoff:     true,
	})
	defer h.close()

	const (
		nSnap = 20
		nKey  = 10000
	)

	var (
		snaps [nSnap]*Snapshot
		b     = &Batch{}
	)
	for i := range snaps {
		vtail := fmt.Sprintf("VAL%030d", i)
		b.Reset()
		for k := 0; k < nKey; k++ {
			key := fmt.Sprintf("KEY%8d", k)
			b.Put([]byte(key), []byte(key+vtail))
		}
		h.stor.EmulateError(testutil.ModeOpen|testutil.ModeRead, storage.TypeTable, errors.New("table transient read error"))
		if err := h.db.Write(b, nil); err != nil {
			t.Logf("WRITE #%d error: %v", i, err)
			h.stor.EmulateError(testutil.ModeOpen|testutil.ModeRead, storage.TypeTable, nil)
			for {
				if err := h.db.Write(b, nil); err == nil {
					break
				} else if errors.IsCorrupted(err) {
					t.Fatalf("WRITE #%d corrupted: %v", i, err)
				}
			}
		}

		snaps[i] = h.db.newSnapshot()
		b.Reset()
		for k := 0; k < nKey; k++ {
			key := fmt.Sprintf("KEY%8d", k)
			b.Delete([]byte(key))
		}
		h.stor.EmulateError(testutil.ModeOpen|testutil.ModeRead, storage.TypeTable, errors.New("table transient read error"))
		if err := h.db.Write(b, nil); err != nil {
			t.Logf("WRITE #%d  error: %v", i, err)
			h.stor.EmulateError(testutil.ModeOpen|testutil.ModeRead, storage.TypeTable, nil)
			for {
				if err := h.db.Write(b, nil); err == nil {
					break
				} else if errors.IsCorrupted(err) {
					t.Fatalf("WRITE #%d corrupted: %v", i, err)
				}
			}
		}
	}
	h.stor.EmulateError(testutil.ModeOpen|testutil.ModeRead, storage.TypeTable, nil)

	runtime.GOMAXPROCS(runtime.NumCPU())

	rnd := rand.New(rand.NewSource(0xecafdaed))
	wg := &sync.WaitGroup{}
	for i, snap := range snaps {
		wg.Add(2)

		go func(i int, snap *Snapshot, sk []int) {
			defer wg.Done()

			vtail := fmt.Sprintf("VAL%030d", i)
			for _, k := range sk {
				if t.Failed() {
					return
				}

				key := fmt.Sprintf("KEY%8d", k)
				xvalue, err := snap.Get([]byte(key), nil)
				if err != nil {
					t.Errorf("READER_GET #%d SEQ=%d K%d error: %v", i, snap.elem.seq, k, err)
					return
				}
				value := key + vtail
				if !bytes.Equal([]byte(value), xvalue) {
					t.Errorf("READER_GET #%d SEQ=%d K%d invalid value: want %q, got %q", i, snap.elem.seq, k, value, xvalue)
					return
				}
			}
		}(i, snap, rnd.Perm(nKey))

		go func(i int, snap *Snapshot) {
			defer wg.Done()

			vtail := fmt.Sprintf("VAL%030d", i)
			iter := snap.NewIterator(nil, nil)
			defer iter.Release()
			for k := 0; k < nKey; k++ {
				if t.Failed() {
					return
				}

				if !iter.Next() {
					if err := iter.Error(); err != nil {
						t.Errorf("READER_ITER #%d K%d error: %v", i, k, err)
						return
					}
					t.Errorf("READER_ITER #%d K%d eoi", i, k)
					return
				}
				key := fmt.Sprintf("KEY%8d", k)
				xkey := iter.Key()
				if !bytes.Equal([]byte(key), xkey) {
					t.Errorf("READER_ITER #%d K%d invalid key: want %q, got %q", i, k, key, xkey)
					return
				}
				value := key + vtail
				xvalue := iter.Value()
				if !bytes.Equal([]byte(value), xvalue) {
					t.Errorf("READER_ITER #%d K%d invalid value: want %q, got %q", i, k, value, xvalue)
					return
				}
			}
		}(i, snap)
	}

	wg.Wait()
}

func TestDB_UkeyShouldntHopAcrossTable(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  112 * opt.KiB,
		CompactionTableSize:          90 * opt.KiB,
		CompactionExpandLimitFactor:  1,
	})
	defer h.close()

	const (
		nSnap = 190
		nKey  = 140
	)

	var (
		snaps [nSnap]*Snapshot
		b     = &Batch{}
	)
	for i := range snaps {
		vtail := fmt.Sprintf("VAL%030d", i)
		b.Reset()
		for k := 0; k < nKey; k++ {
			key := fmt.Sprintf("KEY%08d", k)
			b.Put([]byte(key), []byte(key+vtail))
		}
		if err := h.db.Write(b, nil); err != nil {
			t.Fatalf("WRITE #%d error: %v", i, err)
		}

		snaps[i] = h.db.newSnapshot()
		b.Reset()
		for k := 0; k < nKey; k++ {
			key := fmt.Sprintf("KEY%08d", k)
			b.Delete([]byte(key))
		}
		if err := h.db.Write(b, nil); err != nil {
			t.Fatalf("WRITE #%d  error: %v", i, err)
		}
	}

	h.compactMem()

	h.waitCompaction()
	for level, tables := range h.db.s.stVersion.levels {
		for _, table := range tables {
			t.Logf("L%d@%d %q:%q", level, table.fd.Num, table.imin, table.imax)
		}
	}

	h.compactRangeAt(0, "", "")
	h.waitCompaction()
	for level, tables := range h.db.s.stVersion.levels {
		for _, table := range tables {
			t.Logf("L%d@%d %q:%q", level, table.fd.Num, table.imin, table.imax)
		}
	}
	h.compactRangeAt(1, "", "")
	h.waitCompaction()
	for level, tables := range h.db.s.stVersion.levels {
		for _, table := range tables {
			t.Logf("L%d@%d %q:%q", level, table.fd.Num, table.imin, table.imax)
		}
	}
	runtime.GOMAXPROCS(runtime.NumCPU())

	wg := &sync.WaitGroup{}
	for i, snap := range snaps {
		wg.Add(1)

		go func(i int, snap *Snapshot) {
			defer wg.Done()

			vtail := fmt.Sprintf("VAL%030d", i)
			for k := 0; k < nKey; k++ {
				if t.Failed() {
					return
				}

				key := fmt.Sprintf("KEY%08d", k)
				xvalue, err := snap.Get([]byte(key), nil)
				if err != nil {
					t.Errorf("READER_GET #%d SEQ=%d K%d error: %v", i, snap.elem.seq, k, err)
					return
				}
				value := key + vtail
				if !bytes.Equal([]byte(value), xvalue) {
					t.Errorf("READER_GET #%d SEQ=%d K%d invalid value: want %q, got %q", i, snap.elem.seq, k, value, xvalue)
					return
				}
			}
		}(i, snap)
	}

	wg.Wait()
}

func TestDB_TableCompactionBuilder(t *testing.T) {
	gomega.RegisterTestingT(t)
	stor := testutil.NewStorage()
	stor.OnLog(testingLogger(t))
	stor.OnClose(testingPreserveOnFailed(t))
	defer stor.Close()

	const nSeq = 99

	o := &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  112 * opt.KiB,
		CompactionTableSize:          43 * opt.KiB,
		CompactionExpandLimitFactor:  1,
		CompactionGPOverlapsFactor:   1,
		DisableBlockCache:            true,
	}
	s, err := newSession(stor, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.create(); err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var (
		seq        uint64
		targetSize = 5 * o.CompactionTableSize
		value      = bytes.Repeat([]byte{'0'}, 100)
	)
	for i := 0; i < 2; i++ {
		tw, err := s.tops.create()
		if err != nil {
			t.Fatal(err)
		}
		for k := 0; tw.tw.BytesLen() < targetSize; k++ {
			key := []byte(fmt.Sprintf("%09d", k))
			seq += nSeq - 1
			for x := uint64(0); x < nSeq; x++ {
				if err := tw.append(makeInternalKey(nil, key, seq-x, keyTypeVal), value); err != nil {
					t.Fatal(err)
				}
			}
		}
		tf, err := tw.finish()
		if err != nil {
			t.Fatal(err)
		}
		rec := &sessionRecord{}
		rec.addTableFile(i, tf)
		if err := s.commit(rec, false); err != nil {
			t.Fatal(err)
		}
	}

	// Build grandparent.
	v := s.version()
	c := newCompaction(s, v, 1, append(tFiles{}, v.levels[1]...), undefinedCompaction)
	rec := &sessionRecord{}
	b := &tableCompactionBuilder{
		s:         s,
		c:         c,
		rec:       rec,
		stat1:     new(cStatStaging),
		minSeq:    0,
		strict:    true,
		tableSize: o.CompactionTableSize/3 + 961,
	}
	if err := b.run(new(compactionTransactCounter)); err != nil {
		t.Fatal(err)
	}
	for _, t := range c.levels[0] {
		rec.delTable(c.sourceLevel, t.fd.Num)
	}
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	c.release()

	// Build level-1.
	v = s.version()
	c = newCompaction(s, v, 0, append(tFiles{}, v.levels[0]...), undefinedCompaction)
	rec = &sessionRecord{}
	b = &tableCompactionBuilder{
		s:         s,
		c:         c,
		rec:       rec,
		stat1:     new(cStatStaging),
		minSeq:    0,
		strict:    true,
		tableSize: o.CompactionTableSize,
	}
	if err := b.run(new(compactionTransactCounter)); err != nil {
		t.Fatal(err)
	}
	for _, t := range c.levels[0] {
		rec.delTable(c.sourceLevel, t.fd.Num)
	}
	// Move grandparent to level-3
	for _, t := range v.levels[2] {
		rec.delTable(2, t.fd.Num)
		rec.addTableFile(3, t)
	}
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	c.release()

	v = s.version()
	for level, want := range []bool{false, true, false, true} {
		got := len(v.levels[level]) > 0
		if want != got {
			t.Fatalf("invalid level-%d tables len: want %v, got %v", level, want, got)
		}
	}
	for i, f := range v.levels[1][:len(v.levels[1])-1] {
		nf := v.levels[1][i+1]
		if bytes.Equal(f.imax.ukey(), nf.imin.ukey()) {
			t.Fatalf("KEY %q hop across table %d .. %d", f.imax.ukey(), f.fd.Num, nf.fd.Num)
		}
	}
	v.release()

	// Compaction with transient error.
	v = s.version()
	c = newCompaction(s, v, 1, append(tFiles{}, v.levels[1]...), undefinedCompaction)
	rec = &sessionRecord{}
	b = &tableCompactionBuilder{
		s:         s,
		c:         c,
		rec:       rec,
		stat1:     new(cStatStaging),
		minSeq:    0,
		strict:    true,
		tableSize: o.CompactionTableSize,
	}
	stor.EmulateErrorOnce(testutil.ModeSync, storage.TypeTable, errors.New("table sync error (once)"))
	stor.EmulateRandomError(testutil.ModeRead|testutil.ModeWrite, storage.TypeTable, 0.01, errors.New("table random IO error"))
	for {
		if err := b.run(new(compactionTransactCounter)); err != nil {
			t.Logf("(expected) b.run: %v", err)
		} else {
			break
		}
	}
	if err := s.commit(rec, false); err != nil {
		t.Fatal(err)
	}
	c.release()

	stor.EmulateErrorOnce(testutil.ModeSync, storage.TypeTable, nil)
	stor.EmulateRandomError(testutil.ModeRead|testutil.ModeWrite, storage.TypeTable, 0, nil)

	v = s.version()
	if len(v.levels[1]) != len(v.levels[2]) {
		t.Fatalf("invalid tables length, want %d, got %d", len(v.levels[1]), len(v.levels[2]))
	}
	for i, f0 := range v.levels[1] {
		f1 := v.levels[2][i]
		iter0 := s.tops.newIterator(f0, nil, nil)
		iter1 := s.tops.newIterator(f1, nil, nil)
		for j := 0; true; j++ {
			next0 := iter0.Next()
			next1 := iter1.Next()
			if next0 != next1 {
				t.Fatalf("#%d.%d invalid eoi: want %v, got %v", i, j, next0, next1)
			}
			key0 := iter0.Key()
			key1 := iter1.Key()
			if !bytes.Equal(key0, key1) {
				t.Fatalf("#%d.%d invalid key: want %q, got %q", i, j, key0, key1)
			}
			if next0 == false {
				break
			}
		}
		iter0.Release()
		iter1.Release()
	}
	v.release()
}

func testDB_IterTriggeredCompaction(t *testing.T, limitDiv int) {
	const (
		vSize = 200 * opt.KiB
		tSize = 100 * opt.MiB
		mIter = 100
		n     = tSize / vSize
	)

	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
		DisableBlockCache:            true,
	})
	defer h.close()

	h.db.memdbMaxLevel = 2

	key := func(x int) string {
		return fmt.Sprintf("v%06d", x)
	}

	// Fill.
	value := strings.Repeat("x", vSize)
	for i := 0; i < n; i++ {
		h.put(key(i), value)
	}
	h.compactMem()

	// Delete all.
	for i := 0; i < n; i++ {
		h.delete(key(i))
	}
	h.compactMem()

	var (
		limit = n / limitDiv

		startKey = key(0)
		limitKey = key(limit)
		maxKey   = key(n)
		slice    = &util.Range{Limit: []byte(limitKey)}

		initialSize0 = h.sizeOf(startKey, limitKey)
		initialSize1 = h.sizeOf(limitKey, maxKey)
	)

	t.Logf("initial size %s [rest %s]", shortenb(initialSize0), shortenb(initialSize1))

	for r := 0; true; r++ {
		if r >= mIter {
			t.Fatal("taking too long to compact")
		}

		// Iterates.
		iter := h.db.NewIterator(slice, h.ro)
		for iter.Next() {
		}
		if err := iter.Error(); err != nil {
			t.Fatalf("Iter err: %v", err)
		}
		iter.Release()

		// Wait compaction.
		h.waitCompaction()

		// Check size.
		size0 := h.sizeOf(startKey, limitKey)
		size1 := h.sizeOf(limitKey, maxKey)
		t.Logf("#%03d size %s [rest %s]", r, shortenb(size0), shortenb(size1))
		if size0 < initialSize0/10 {
			break
		}
	}

	if initialSize1 > 0 {
		h.sizeAssert(limitKey, maxKey, initialSize1/4-opt.MiB, initialSize1+opt.MiB)
	}
}

func TestDB_IterTriggeredCompaction(t *testing.T) {
	testDB_IterTriggeredCompaction(t, 1)
}

func TestDB_IterTriggeredCompactionHalf(t *testing.T) {
	testDB_IterTriggeredCompaction(t, 2)
}

func TestDB_ReadOnly(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	h.put("foo", "v1")
	h.put("bar", "v2")
	h.compactMem()

	h.put("xfoo", "v1")
	h.put("xbar", "v2")

	t.Log("Trigger read-only")
	if err := h.db.SetReadOnly(); err != nil {
		h.close()
		t.Fatalf("SetReadOnly error: %v", err)
	}

	mode := testutil.ModeCreate | testutil.ModeRemove | testutil.ModeRename | testutil.ModeWrite | testutil.ModeSync
	h.stor.EmulateError(mode, storage.TypeAll, errors.New("read-only DB shouldn't writes"))

	ro := func(key, value, wantValue string) {
		if err := h.db.Put([]byte(key), []byte(value), h.wo); err != ErrReadOnly {
			t.Fatalf("unexpected error: %v", err)
		}
		h.getVal(key, wantValue)
	}

	ro("foo", "vx", "v1")

	h.o.ReadOnly = true
	h.reopenDB()

	ro("foo", "vx", "v1")
	ro("bar", "vx", "v2")
	h.assertNumKeys(4)
}

func TestDB_BulkInsertDelete(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
		CompactionTableSize:          128 * opt.KiB,
		CompactionTotalSize:          1 * opt.MiB,
		WriteBuffer:                  256 * opt.KiB,
	})
	defer h.close()

	const R = 100
	const N = 2500
	key := make([]byte, 4)
	value := make([]byte, 256)
	for i := 0; i < R; i++ {
		offset := N * i
		for j := 0; j < N; j++ {
			binary.BigEndian.PutUint32(key, uint32(offset+j))
			if err := h.db.Put(key, value, nil); err != nil {
				t.Fatal("Put error: ", err)
			}
		}
		for j := 0; j < N; j++ {
			binary.BigEndian.PutUint32(key, uint32(offset+j))
			if err := h.db.Delete(key, nil); err != nil {
				t.Fatal("Delete error: ", err)
			}
		}
	}

	h.waitCompaction()
	if tot := h.totalTables(); tot > 10 {
		t.Fatalf("too many uncompacted tables: %d (%s)", tot, h.getTablesPerLevel())
	}
}

func TestDB_GracefulClose(t *testing.T) {
	runtime.GOMAXPROCS(4)
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
		CompactionTableSize:          1 * opt.MiB,
		WriteBuffer:                  1 * opt.MiB,
	})
	defer h.close()

	var closeWait sync.WaitGroup

	// During write.
	n := 0
	closing := false
	for i := 0; i < 1000000; i++ {
		if !closing && h.totalTables() > 3 {
			t.Logf("close db during write, index=%d", i)
			closeWait.Add(1)
			go func() {
				h.closeDB()
				closeWait.Done()
			}()
			closing = true
		}
		if err := h.db.Put([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprintf("VAL-%09d", i)), h.wo); err != nil {
			t.Logf("Put error: %s (expected)", err)
			n = i
			break
		}
	}
	closeWait.Wait()

	// During read.
	h.openDB()
	closing = false
	for i := 0; i < n; i++ {
		if !closing && i > n/2 {
			t.Logf("close db during read, index=%d", i)
			closeWait.Add(1)
			go func() {
				h.closeDB()
				closeWait.Done()
			}()
			closing = true
		}
		if _, err := h.db.Get([]byte(fmt.Sprintf("%09d", i)), h.ro); err != nil {
			t.Logf("Get error: %s (expected)", err)
			break
		}
	}
	closeWait.Wait()

	// During iterate.
	h.openDB()
	closing = false
	iter := h.db.NewIterator(nil, h.ro)
	for i := 0; iter.Next(); i++ {
		if len(iter.Key()) == 0 || len(iter.Value()) == 0 {
			t.Error("Key or value has zero length")
		}
		if !closing {
			t.Logf("close db during iter, index=%d", i)
			closeWait.Add(1)
			go func() {
				h.closeDB()
				closeWait.Done()
			}()
			closing = true
		}
		time.Sleep(time.Millisecond)
	}
	if err := iter.Error(); err != nil {
		t.Logf("Iter error: %s (expected)", err)
	}
	iter.Release()
	closeWait.Wait()
}
