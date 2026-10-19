// Copyright (c) 2026, Martin Minka <martin.minka@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gcash/goleveldb/leveldb/errors"
	"github.com/gcash/goleveldb/leveldb/journal"
	"github.com/gcash/goleveldb/leveldb/opt"
	"github.com/gcash/goleveldb/leveldb/util"
)

// Regression tests for journal recovery dropping records that the reference
// C++ implementation replays, and for the read-write open then deleting the
// journal so the loss becomes permanent.
//
// The trigger is a disagreement about what the manifest's last-sequence field
// means relative to the journal the manifest names.
//
//	                     journal 000003 opened here
//	                     |
//	seq  ... 9  10 | 11  12  13  14  15  16 ... 20
//	               |         batches in journal 000003
//	               ^                   ^
//	               frozenSeq           liveSeq
//	               what goleveldb      what the reference
//	               records             records
//
// The reference implementation reaches the second state routinely. When a
// memtable fills it opens a new journal and keeps accepting writes into it;
// the flush of the old memtable runs in the background with the lock released.
// When that flush finishes, CompactMemTable records log_number = the journal
// currently being written (db_impl.cc), and LogAndApply records last_sequence =
// last_sequence_, the live counter, which the concurrent writes have already
// advanced (version_set.cc). So the manifest names a journal whose first record
// is older than the sequence the manifest carries.
//
// The reference does not mind: RecoverLogFile replays every batch in the
// journal and only ever raises max_sequence (db_impl.cc).
//
// goleveldb did mind. It seeds db.seq from the manifest and decodeBatchToMem
// rejected any batch below it as corrupt:
//
//	if seq < expectSeq {
//	        return 0, 0, newErrBatchCorrupted("invalid sequence number")
//	}
//
// and recoverJournal skips corrupt batches without failing the open, because
// opt.StrictJournal is set neither by DefaultStrict nor by NoStrict. So the
// records went with no error and no trace an application could observe.
// A goleveldb memtable flush records frozenSeq, the sequence at rotation,
// rather than the live counter, but goleveldb's own writers produced this
// shape too: a transaction opened while a flush was pending recorded its own,
// higher sequence, and if the DB closed or crashed before the flush
// committed, the frozen journal's batches were below it. OpenTransaction now
// waits for the flush.

const (
	// The journal holds one batch per sequence in [firstJournalSeq,
	// lastJournalSeq]; each batch writes the single key named for its sequence.
	firstJournalSeq = 11
	lastJournalSeq  = 20

	// frozenSeqFixture is the sequence the rotated memtable ended at, which
	// is what a goleveldb writer puts in the manifest.
	frozenSeqFixture = 10

	// liveSeqFixture is where concurrent writes had taken the live counter by
	// the time the background flush wrote the manifest, which is what the
	// reference puts there.
	liveSeqFixture = 15

	fixtureManifestNum = 2
	fixtureJournalNum  = 3
	fixtureNextFileNum = 4
)

// Tags in a VersionEdit record, from version_edit.cc. session_record.go decodes
// the same numbers; they are spelled out here so the fixture describes the
// on-disk format rather than the code under test.
const (
	fixtureTagComparator     = 1
	fixtureTagLogNumber      = 2
	fixtureTagNextFileNumber = 3
	fixtureTagLastSequence   = 4
)

// Record types inside a write batch, from dbformat.h. Same reasoning as above:
// these are keyTypeDel and keyTypeVal, written out independently.
const (
	fixtureTypeDeletion = 0
	fixtureTypeValue    = 1
)

// fixtureComparator is the only comparator name goleveldb will open a directory
// under; a mismatch is rejected outright in session.recover.
const fixtureComparator = "leveldb.BytewiseComparator"

// fixtureEntry is one key/value record inside a fixtureBatch.
type fixtureEntry struct {
	key    string
	value  string
	delete bool // encode as a deletion; value is ignored
}

// fixtureBatch is one write batch as it appears in a journal file. seq is the
// sequence number assigned to its first entry.
type fixtureBatch struct {
	seq     uint64
	entries []fixtureEntry
}

// recoveryFixture describes a LevelDB directory written from scratch: one
// manifest naming one journal, and the batches that journal holds. The
// interesting states are the ones where lastSequence does not sit where a
// goleveldb writer would have put it.
//
// The three file formats are small enough to write directly:
//
//   - CURRENT holds the current manifest's file name and a newline.
//   - MANIFEST-N is a journal-framed file holding VersionEdit records, each a
//     sequence of varint-tagged fields (version_edit.cc).
//   - N.log is a journal-framed file holding write batches: an 8-byte sequence
//     number, a 4-byte record count, then that many key/value records
//     (write_batch.cc).
//
// Journal framing itself is not what is under test, so journal.Writer writes it.
type recoveryFixture struct {
	manifestNum  uint64
	journalNum   uint64
	nextFileNum  uint64
	lastSequence uint64
	batches      []fixtureBatch
}

func (f recoveryFixture) journalName() string {
	return fmt.Sprintf("%06d.log", f.journalNum)
}

func (f recoveryFixture) manifestName() string {
	return fmt.Sprintf("MANIFEST-%06d", f.manifestNum)
}

// write creates dir and writes the fixture's CURRENT, manifest and journal.
func (f recoveryFixture) write(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create fixture dir: %v", err)
	}
	if err := f.writeManifest(dir); err != nil {
		return err
	}
	if err := f.writeJournal(dir); err != nil {
		return err
	}
	current := []byte(f.manifestName() + "\n")
	if err := os.WriteFile(filepath.Join(dir, "CURRENT"), current, 0o644); err != nil {
		return fmt.Errorf("write CURRENT: %v", err)
	}
	return nil
}

// writeManifest writes a single VersionEdit carrying the four fields goleveldb
// requires to be present: comparator, log number, next file number and last
// sequence. A manifest omitting any of them is rejected as corrupt rather than
// recovered.
func (f recoveryFixture) writeManifest(dir string) error {
	var rec bytes.Buffer
	fixturePutUvarint(&rec, fixtureTagComparator)
	fixturePutLenPrefixed(&rec, fixtureComparator)
	fixturePutUvarint(&rec, fixtureTagLogNumber)
	fixturePutUvarint(&rec, f.journalNum)
	fixturePutUvarint(&rec, fixtureTagNextFileNumber)
	fixturePutUvarint(&rec, f.nextFileNum)
	fixturePutUvarint(&rec, fixtureTagLastSequence)
	fixturePutUvarint(&rec, f.lastSequence)

	return fixtureWriteJournalFile(filepath.Join(dir, f.manifestName()), [][]byte{rec.Bytes()})
}

// writeJournal writes the fixture's batches into the journal file, one journal
// record per batch, which is how a write per transaction lands on disk.
func (f recoveryFixture) writeJournal(dir string) error {
	return fixtureWriteBatches(filepath.Join(dir, f.journalName()), f.batches)
}

// fixtureWriteBatches writes batches into the journal file at path, one
// journal record per batch.
func fixtureWriteBatches(path string, batches []fixtureBatch) error {
	records := make([][]byte, 0, len(batches))
	for i, b := range batches {
		rec, err := fixtureEncodeBatch(b)
		if err != nil {
			return fmt.Errorf("encode batch %d: %v", i, err)
		}
		records = append(records, rec)
	}
	return fixtureWriteJournalFile(path, records)
}

// fixtureEncodeBatch lays out one write batch: sequence number, record count,
// then the records themselves. The count field is 32 bits wide on disk.
func fixtureEncodeBatch(b fixtureBatch) ([]byte, error) {
	if uint64(len(b.entries)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%d entries exceeds the batch count field", len(b.entries))
	}

	var buf bytes.Buffer
	var header [batchHeaderLen]byte
	binary.LittleEndian.PutUint64(header[:8], b.seq)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(b.entries)))
	buf.Write(header[:])

	for _, e := range b.entries {
		if e.delete {
			buf.WriteByte(fixtureTypeDeletion)
			fixturePutLenPrefixed(&buf, e.key)
			continue
		}
		buf.WriteByte(fixtureTypeValue)
		fixturePutLenPrefixed(&buf, e.key)
		fixturePutLenPrefixed(&buf, e.value)
	}
	return buf.Bytes(), nil
}

// fixtureWriteJournalFile writes records into path using LevelDB's journal
// framing, which both the manifest and the write-ahead log use.
func fixtureWriteJournalFile(path string, records [][]byte) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %v", filepath.Base(path), err)
	}

	w := journal.NewWriter(file)
	for i, rec := range records {
		dst, err := w.Next()
		if err != nil {
			file.Close()
			return fmt.Errorf("start record %d in %s: %v", i, filepath.Base(path), err)
		}
		if _, err := dst.Write(rec); err != nil {
			file.Close()
			return fmt.Errorf("write record %d in %s: %v", i, filepath.Base(path), err)
		}
	}
	if err := w.Close(); err != nil {
		file.Close()
		return fmt.Errorf("flush %s: %v", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %v", filepath.Base(path), err)
	}
	return nil
}

func fixturePutUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func fixturePutLenPrefixed(buf *bytes.Buffer, s string) {
	fixturePutUvarint(buf, uint64(len(s)))
	buf.WriteString(s)
}

func fixtureSeqKey(seq uint64) string { return fmt.Sprintf("key-%02d", seq) }

// fixtureSeqBatches returns one batch per sequence in [from, to], each
// writing the single key named for its sequence.
func fixtureSeqBatches(from, to uint64) []fixtureBatch {
	var batches []fixtureBatch
	for seq := from; seq <= to; seq++ {
		batches = append(batches, fixtureBatch{
			seq:     seq,
			entries: []fixtureEntry{{key: fixtureSeqKey(seq), value: "v"}},
		})
	}
	return batches
}

// newRecoveryFixture builds the directory described at the top of this file,
// with the manifest's last-sequence field as the single variable under test.
func newRecoveryFixture(lastSequence uint64) recoveryFixture {
	return recoveryFixture{
		manifestNum:  fixtureManifestNum,
		journalNum:   fixtureJournalNum,
		nextFileNum:  fixtureNextFileNum,
		lastSequence: lastSequence,
		batches:      fixtureSeqBatches(firstJournalSeq, lastJournalSeq),
	}
}

// writeRecoveryFixture writes f into a fresh temporary directory.
func writeRecoveryFixture(t *testing.T, f recoveryFixture) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	if err := f.write(dir); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return dir
}

// recoveredFixtureKeys opens dir and returns every key the database reports.
func recoveredFixtureKeys(t *testing.T, dir string, o *opt.Options) []string {
	t.Helper()
	db, err := OpenFile(dir, o)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close %s: %v", dir, err)
		}
	}()

	var keys []string
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		t.Fatalf("iterate %s: %v", dir, err)
	}
	return keys
}

// wantFixtureKeys lists the keys for sequences in [from, lastJournalSeq].
func wantFixtureKeys(from uint64) []string {
	var keys []string
	for seq := from; seq <= lastJournalSeq; seq++ {
		keys = append(keys, fixtureSeqKey(seq))
	}
	return keys
}

func assertFixtureKeys(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("recovered %d keys, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("recovered key %d = %q, want %q\n got: %v\nwant: %v", i, got[i], want[i], got, want)
			return
		}
	}
}

// TestJournalRecoveryBelowManifestSequence is the reproducer. Before the fix,
// the reference-shaped fixture came back missing every batch below the
// manifest's last-sequence field, with no error; both shapes must return all
// ten keys.
func TestJournalRecoveryBelowManifestSequence(t *testing.T) {
	writers := []struct {
		name         string
		lastSequence uint64
	}{
		{"goleveldb_writer_records_the_frozen_sequence", frozenSeqFixture},
		{"reference_writer_records_the_live_sequence", liveSeqFixture},
	}
	// Relaxing strictness makes no difference, and cannot: opt.StrictJournal is
	// absent from DefaultStrict and cleared by NoStrict alike, so both take the
	// silent-skip branch.
	modes := []struct {
		name    string
		options *opt.Options
	}{
		{"default_options", &opt.Options{ReadOnly: true}},
		{"NoStrict", &opt.Options{ReadOnly: true, Strict: opt.NoStrict}},
	}

	for _, w := range writers {
		for _, m := range modes {
			t.Run(w.name+"/"+m.name, func(t *testing.T) {
				dir := writeRecoveryFixture(t, newRecoveryFixture(w.lastSequence))
				got := recoveredFixtureKeys(t, dir, m.options)
				// Every batch in the journal must come back, whatever the
				// manifest's last-sequence field says. The reference returns
				// all ten in both shapes.
				assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
			})
		}
	}
}

// TestJournalRecoveryStrictAcceptsBelowManifestSequence guards against the fix
// being mistaken for leniency. Before it, opening this fixture under
// opt.StrictJournal failed with "invalid sequence number", the same rejection
// DefaultStrict turned into a silent skip. The batches are not corrupt, so
// strict journal recovery must accept them and return everything.
func TestJournalRecoveryStrictAcceptsBelowManifestSequence(t *testing.T) {
	dir := writeRecoveryFixture(t, newRecoveryFixture(liveSeqFixture))
	got := recoveredFixtureKeys(t, dir, &opt.Options{ReadOnly: true, Strict: opt.StrictJournal})
	assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
}

// TestJournalRecoveryAlwaysVerifiesChecksums ensures recovery verifies journal
// checksums whatever opt.Strict holds, as LevelDB's RecoverLogFile does, so a
// corrupted batch is dropped, or fails the open under StrictJournal, rather
// than replayed. Any Strict value without StrictJournalChecksum, NoStrict
// included, used to turn checksums off, so a batch whose sequence a bit flip
// had changed was applied at the wrong sequence.
func TestJournalRecoveryAlwaysVerifiesChecksums(t *testing.T) {
	// Flip a bit in the last batch's sequence, which leaves its checksum
	// stale. The last batch ends the journal file.
	corrupted := func(t *testing.T) string {
		fixture := newRecoveryFixture(frozenSeqFixture)
		dir := writeRecoveryFixture(t, fixture)
		path := filepath.Join(dir, fixture.journalName())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		last, err := fixtureEncodeBatch(fixture.batches[len(fixture.batches)-1])
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)-len(last)] ^= 0x10 // sequence 20 becomes 4
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	// Read-only opens recover through their own loop, which verifies them too.
	for _, readOnly := range []bool{false, true} {
		for _, strict := range []opt.Strict{opt.NoStrict, opt.StrictManifest} {
			got := recoveredFixtureKeys(t, corrupted(t), &opt.Options{Strict: strict, ReadOnly: readOnly})
			assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq)[:lastJournalSeq-firstJournalSeq])
		}

		db, err := OpenFile(corrupted(t), &opt.Options{Strict: opt.StrictJournal, ReadOnly: readOnly})
		if err == nil {
			db.Close()
			t.Errorf("open (read-only %v) with a corrupted journal under StrictJournal: expected error", readOnly)
		} else if !errors.IsCorrupted(err) {
			t.Errorf("open (read-only %v) with a corrupted journal under StrictJournal = %v, want a corruption error", readOnly, err)
		}
	}
}

// TestJournalRecoveryMalformedBatchNotHalfApplied ensures a batch whose header
// is sound but one of whose records is not is skipped whole. Recovery used to
// insert its records up to the bad one before rejecting it, without moving
// the sequence past them, so later writes reused their sequence numbers.
func TestJournalRecoveryMalformedBatchNotHalfApplied(t *testing.T) {
	fixture := newRecoveryFixture(frozenSeqFixture)
	fixture.batches = fixtureSeqBatches(firstJournalSeq, 15)
	dir := writeRecoveryFixture(t, fixture)

	// Rewrite the journal with a last batch at 16 that claims two records but
	// holds one good one and a record of an unknown type. Its checksum is
	// valid, so only decoding finds the fault.
	var records [][]byte
	for _, b := range fixture.batches {
		rec, err := fixtureEncodeBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, rec)
	}
	half, err := fixtureEncodeBatch(fixtureBatch{seq: 16, entries: []fixtureEntry{{key: "half", value: "v"}}})
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(half[8:], 2)
	records = append(records, append(half, 0x7f))
	if err := fixtureWriteJournalFile(filepath.Join(dir, fixture.journalName()), records); err != nil {
		t.Fatal(err)
	}

	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Get([]byte("half"), nil); err != errors.ErrNotFound {
		t.Errorf("get half = %v, want not found: its batch is malformed", err)
	}
	for seq := uint64(firstJournalSeq); seq <= 15; seq++ {
		if _, err := db.Get([]byte(fixtureSeqKey(seq)), nil); err != nil {
			t.Errorf("get %s: %v", fixtureSeqKey(seq), err)
		}
	}
}

// TestJournalRecoveryBatchLengthOverflowSkipped ensures recovery treats a
// batch whose key length overflows an int as corrupt: it is skipped, or fails
// the open under StrictJournal. The length passed the decoder's check as a
// negative int and recovery panicked, so neither Open nor Recover could open
// the DB.
func TestJournalRecoveryBatchLengthOverflowSkipped(t *testing.T) {
	corrupted := func(t *testing.T) string {
		fixture := newRecoveryFixture(frozenSeqFixture)
		fixture.batches = fixtureSeqBatches(firstJournalSeq, 15)
		dir := writeRecoveryFixture(t, fixture)

		// Rewrite the journal with a last batch at 16 holding a delete whose
		// key length is 2^63. Its checksum is valid, so only decoding finds
		// the fault.
		var records [][]byte
		for _, b := range fixture.batches {
			rec, err := fixtureEncodeBatch(b)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, rec)
		}
		bad := append(encodeBatchHeader(nil, 16, 1), 0x00, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
		records = append(records, bad)
		if err := fixtureWriteJournalFile(filepath.Join(dir, fixture.journalName()), records); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	db, err := OpenFile(corrupted(t), &opt.Options{Strict: opt.StrictJournal})
	if err == nil {
		db.Close()
		t.Error("open under StrictJournal: expected a corruption error")
	} else if !errors.IsCorrupted(err) {
		t.Errorf("open under StrictJournal = %v, want a corruption error", err)
	}

	db, err = OpenFile(corrupted(t), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	for seq := uint64(firstJournalSeq); seq <= 15; seq++ {
		if _, err := db.Get([]byte(fixtureSeqKey(seq)), nil); err != nil {
			t.Errorf("get %s: %v", fixtureSeqKey(seq), err)
		}
	}
}

// TestManifestCutRecordAppliesNothing ensures a MANIFEST record cut off at a
// block boundary, as a flush commit whose Flush failed after its first block
// reached the file leaves it, applies none of its fields. Recovery kept the
// fields decoded before the cut: the record's journal number made it skip the
// journal whose batches the record's table never got to hold, and the janitor
// then removed that journal.
func TestManifestCutRecordAppliesNothing(t *testing.T) {
	fixture := newRecoveryFixture(frozenSeqFixture)
	dir := writeRecoveryFixture(t, fixture)
	writeCutManifest(t, dir, fixture, true)
	assertFixtureKeys(t, recoveredFixtureKeys(t, dir, nil), wantFixtureKeys(firstJournalSeq))
}

// writeCutManifest rewrites the fixture's MANIFEST in dir: the fixture's
// record, then a flush commit for a newer journal that the end of the file
// cuts off, as a commit whose write failed partway leaves it. With boundary,
// a filler puts the commit across the first block boundary and the file ends
// there, so the commit's first chunk holds only its journal number and the
// next field's tag. Otherwise the commit sits in the first block, and the
// file ends in its middle.
func writeCutManifest(t *testing.T, dir string, fixture recoveryFixture, boundary bool) {
	t.Helper()
	const (
		blockSize  = 32 * 1024
		headerSize = 7
		firstChunk = 3
	)
	var buf bytes.Buffer
	w := journal.NewWriter(&buf)
	put := func(rec *sessionRecord) {
		dst, err := w.Next()
		if err != nil {
			t.Fatal(err)
		}
		if err := rec.encode(dst); err != nil {
			t.Fatal(err)
		}
	}
	base := &sessionRecord{}
	base.setComparer(fixtureComparator)
	base.setJournalNum(int64(fixture.journalNum))
	base.setNextFileNum(int64(fixture.nextFileNum))
	base.setSeqNum(fixture.lastSequence)
	put(base)

	if boundary {
		at := int64(blockSize - headerSize - firstChunk)
		filler := func(keyLen int) *sessionRecord {
			rec := &sessionRecord{}
			rec.addCompPtr(1, makeInternalKey(nil, bytes.Repeat([]byte{'x'}, keyLen-8), 0, keyTypeVal))
			return rec
		}
		want := int(at - w.Size() - headerSize)
		keyLen := want
		for ; keyLen > 8; keyLen-- {
			var b bytes.Buffer
			if err := filler(keyLen).encode(&b); err != nil {
				t.Fatal(err)
			}
			if b.Len() == want {
				break
			}
		}
		put(filler(keyLen))
		if w.Size() != at {
			t.Fatalf("layout: the cut record starts at %d, want %d", w.Size(), at)
		}
	}

	cut := &sessionRecord{}
	cut.setJournalNum(int64(fixture.journalNum) + 10)
	cut.setNextFileNum(int64(fixture.nextFileNum) + 20)
	cut.setSeqNum(lastJournalSeq)
	cut.addTable(0, int64(fixture.nextFileNum)+15, 1000,
		makeInternalKey(nil, []byte("a"), 1, keyTypeVal), makeInternalKey(nil, []byte("z"), 1, keyTypeVal))
	put(cut)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	end := buf.Len() - 10
	if boundary {
		if buf.Len() <= blockSize {
			t.Fatalf("layout: the cut record ends at %d, within the first block", buf.Len())
		}
		end = blockSize
	}
	if err := os.WriteFile(filepath.Join(dir, fixture.manifestName()), buf.Bytes()[:end], 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestStrictManifestOpensAfterCutRecord ensures an open under StrictManifest
// drops a last MANIFEST record the end of the file cuts off, a commit that
// never finished, and recovers the state before it, as LevelDB does with
// paranoid checks. It failed the open, so after a failed MANIFEST commit,
// which stops writes until the DB is reopened, the reopen failed too.
func TestStrictManifestOpensAfterCutRecord(t *testing.T) {
	for _, test := range []struct {
		name     string
		boundary bool
	}{
		{"cut mid-chunk", false},
		{"cut at a block boundary", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(frozenSeqFixture)
			dir := writeRecoveryFixture(t, fixture)
			writeCutManifest(t, dir, fixture, test.boundary)
			got := recoveredFixtureKeys(t, dir, &opt.Options{Strict: opt.StrictManifest})
			assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
		})
	}
}

// TestStrictJournalOpensAfterCutRecord ensures an open under StrictJournal
// drops a last journal batch the end of the file cuts off, as a crash
// mid-write leaves it, and recovers every batch before it, as LevelDB does
// with paranoid checks. It failed the open as corrupt.
func TestStrictJournalOpensAfterCutRecord(t *testing.T) {
	fixture := newRecoveryFixture(frozenSeqFixture)
	dir := writeRecoveryFixture(t, fixture)
	path := filepath.Join(dir, fixture.journalName())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	last, err := fixtureEncodeBatch(fixture.batches[len(fixture.batches)-1])
	if err != nil {
		t.Fatal(err)
	}
	// Cut the last batch in half.
	if err := os.WriteFile(path, data[:len(data)-len(last)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	got := recoveredFixtureKeys(t, dir, &opt.Options{Strict: opt.StrictJournal})
	assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq)[:lastJournalSeq-firstJournalSeq])
}

// TestJournalRecoveryReadWriteOpenPreservesRecords covers what made the bug
// destructive rather than merely wrong: the skipped records were not just
// invisible, they were destroyed. recoverJournal ends by committing a manifest
// that names a fresh journal and removing the old one, so anything it failed to
// read became unreachable for every later reader, the reference implementation
// included.
//
// The journal is still deleted. That is ordinary LevelDB behaviour and this
// test asserts it, because the deletion is what makes a recovery mistake
// irreversible. What must hold is that everything in it was recovered first.
func TestJournalRecoveryReadWriteOpenPreservesRecords(t *testing.T) {
	fixture := newRecoveryFixture(liveSeqFixture)
	dir := writeRecoveryFixture(t, fixture)
	journalPath := filepath.Join(dir, fixture.journalName())

	db, err := OpenFile(dir, nil) // read-write
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Errorf("journal %s survived the read-write open (stat error: %v); "+
			"recovery mistakes would be reversible and this test no longer covers the bug",
			fixture.journalName(), err)
	}

	// Nothing names that journal any more, so this is the last word on its
	// contents.
	got := recoveredFixtureKeys(t, dir, &opt.Options{ReadOnly: true})
	assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
}

// TestJournalRecoveryDiscardsNothing reads the trace goleveldb leaves in the
// database's LOG file. Before the fix it held one "invalid sequence number
// (skipped)" line per lost record, which was the only signal an operator ever
// got. Recovery must discard nothing, and must still say which batches were
// below the sequence it had reached.
//
// This is what makes the counting tests trustworthy: a count can come out right
// while records are dropped and re-added elsewhere; a clean LOG says the batches
// were never rejected in the first place.
func TestJournalRecoveryDiscardsNothing(t *testing.T) {
	dir := writeRecoveryFixture(t, newRecoveryFixture(liveSeqFixture))

	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	logBytes, err := os.ReadFile(filepath.Join(dir, "LOG"))
	if err != nil {
		t.Fatalf("read LOG: %v", err)
	}
	for _, unwanted := range []string{"invalid sequence number", "(skipped)"} {
		if strings.Contains(string(logBytes), unwanted) {
			t.Errorf("LOG reports %q; recovery is still discarding journal records:\n%s", unwanted, logBytes)
		}
	}

	// Batches 11 to 14 are below the manifest's sequence of 15. One line
	// reports them for the journal, so a large journal cannot flood the LOG.
	want := fmt.Sprintf("replayed N·%d batches below the recovered sequence, lowest Q·%d",
		liveSeqFixture-firstJournalSeq, firstJournalSeq)
	if n := strings.Count(string(logBytes), "below the recovered sequence"); n != 1 ||
		!strings.Contains(string(logBytes), want) {
		t.Errorf("LOG reports batches below the recovered sequence in %d lines, want one with %q:\n%s",
			n, want, logBytes)
	}
}

// TestJournalRecoveryKeepsHighestSequence ensures recovery leaves the
// database's sequence at the highest one replayed, not at the last batch's.
// Otherwise a later write would get a sequence below an older value's, and
// once the sequence passed that value again the older value would win.
func TestJournalRecoveryKeepsHighestSequence(t *testing.T) {
	fixture := newRecoveryFixture(liveSeqFixture)
	fixture.batches = []fixtureBatch{
		{seq: 25, entries: []fixtureEntry{{key: "k", value: "old"}}},
		{seq: 11, entries: []fixtureEntry{{key: "x", value: "v"}}},
	}
	dir := writeRecoveryFixture(t, fixture)

	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	defer db.Close()
	if _, err := db.Get([]byte("x"), nil); err != nil {
		t.Errorf("get x: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("new"), nil); err != nil {
		t.Fatalf("put k: %v", err)
	}
	// Take the sequence well past the old value's, and compact so both values
	// are in tables, where the higher sequence wins.
	for i := 0; i < 20; i++ {
		if err := db.Put([]byte(fmt.Sprintf("other-%02d", i)), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if v, err := db.Get([]byte("k"), nil); err != nil || string(v) != "new" {
		t.Errorf("get k = %q, %v; want new", v, err)
	}
}

// TestJournalRecoveryDeleteBelowManifestSequence ensures a journaled delete
// below the manifest's sequence still hides the value it deleted from a table.
// Dropping the batch brought the deleted value back.
func TestJournalRecoveryDeleteBelowManifestSequence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("old"), nil); err != nil {
		t.Fatalf("put k: %v", err)
	}
	if err := db.CompactRange(util.Range{}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// A manifest listing the table that holds k, at sequence 50, and naming a
	// journal whose only batch deletes k at sequence 40.
	rec := &sessionRecord{}
	v := db.s.version()
	for level, tables := range v.levels {
		for _, t := range tables {
			rec.addTableFile(level, t)
		}
	}
	v.release()
	if len(rec.addedTables) != 1 {
		t.Fatalf("%d tables after compaction, want 1", len(rec.addedTables))
	}
	manifestNum := uint64(db.s.nextFileNum()) + 10
	journalNum := manifestNum + 1
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rec.setComparer(fixtureComparator)
	rec.setJournalNum(int64(journalNum))
	rec.setNextFileNum(int64(journalNum) + 1)
	rec.setSeqNum(50)
	var buf bytes.Buffer
	if err := rec.encode(&buf); err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	fixture := recoveryFixture{manifestNum: manifestNum, journalNum: journalNum}
	if err := fixtureWriteJournalFile(filepath.Join(dir, fixture.manifestName()), [][]byte{buf.Bytes()}); err != nil {
		t.Fatal(err)
	}
	deleteK := fixtureBatch{seq: 40, entries: []fixtureEntry{{key: "k", delete: true}}}
	if err := fixtureWriteBatches(filepath.Join(dir, fixture.journalName()), []fixtureBatch{deleteK}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte(fixture.manifestName()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	db, err = OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if v, err := db.Get([]byte("k"), nil); err != errors.ErrNotFound {
		t.Errorf("get k = %q, %v; want not found", v, err)
	}
}

// TestJournalRecoveryAcrossTwoJournals ensures batches below the manifest's
// sequence are recovered from every journal it names, across the commit
// recovery makes between journals. The read-only open comes first, so that it
// replays both journals: it used to fail with EOF on the second one.
func TestJournalRecoveryAcrossTwoJournals(t *testing.T) {
	fixture := newRecoveryFixture(30)
	fixture.nextFileNum = fixtureJournalNum + 2
	fixture.batches = fixtureSeqBatches(firstJournalSeq, 15)
	dir := writeRecoveryFixture(t, fixture)
	second := filepath.Join(dir, fmt.Sprintf("%06d.log", fixtureJournalNum+1))
	if err := fixtureWriteBatches(second, fixtureSeqBatches(16, lastJournalSeq)); err != nil {
		t.Fatal(err)
	}

	assertFixtureKeys(t, recoveredFixtureKeys(t, dir, &opt.Options{ReadOnly: true}),
		wantFixtureKeys(firstJournalSeq))
	assertFixtureKeys(t, recoveredFixtureKeys(t, dir, nil), wantFixtureKeys(firstJournalSeq))
}
