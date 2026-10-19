// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/gcash/goleveldb/leveldb/errors"
	"github.com/gcash/goleveldb/leveldb/iterator"
	"github.com/gcash/goleveldb/leveldb/opt"
	"github.com/gcash/goleveldb/leveldb/storage"
	"github.com/gcash/goleveldb/leveldb/testutil"
	"github.com/gcash/goleveldb/leveldb/util"
)

type tableWrapper struct {
	*Reader
}

func (t tableWrapper) TestFind(key []byte) (rkey, rvalue []byte, err error) {
	return t.Find(key, false, nil)
}

func (t tableWrapper) TestGet(key []byte) (value []byte, err error) {
	return t.Get(key, nil)
}

func (t tableWrapper) TestNewIterator(slice *util.Range) iterator.Iterator {
	return t.NewIterator(slice, nil)
}

var _ = testutil.Defer(func() {
	Describe("Table", func() {
		Describe("approximate offset test", func() {
			var (
				buf = &bytes.Buffer{}
				o   = &opt.Options{
					BlockSize:   1024,
					Compression: opt.NoCompression,
				}
			)

			// Building the table.
			tw := NewWriter(buf, o, nil, 0)
			err := tw.Append([]byte("k01"), []byte("hello"))
			Expect(err).ShouldNot(HaveOccurred())
			err = tw.Append([]byte("k02"), []byte("hello2"))
			Expect(err).ShouldNot(HaveOccurred())
			err = tw.Append([]byte("k03"), bytes.Repeat([]byte{'x'}, 10000))
			Expect(err).ShouldNot(HaveOccurred())
			err = tw.Append([]byte("k04"), bytes.Repeat([]byte{'x'}, 200000))
			Expect(err).ShouldNot(HaveOccurred())
			err = tw.Append([]byte("k05"), bytes.Repeat([]byte{'x'}, 300000))
			Expect(err).ShouldNot(HaveOccurred())
			err = tw.Append([]byte("k06"), []byte("hello3"))
			Expect(err).ShouldNot(HaveOccurred())
			err = tw.Append([]byte("k07"), bytes.Repeat([]byte{'x'}, 100000))
			Expect(err).ShouldNot(HaveOccurred())

			err = tw.Close()

			It("Should be able to approximate offset of a key correctly", func() {
				Expect(err).ShouldNot(HaveOccurred())

				tr, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), storage.FileDesc{}, nil, nil, o)
				Expect(err).ShouldNot(HaveOccurred())
				CheckOffset := func(key string, expect, threshold int) {
					offset, err := tr.OffsetOf([]byte(key))
					Expect(err).ShouldNot(HaveOccurred())
					Expect(offset).Should(BeNumerically("~", expect, threshold), "Offset of key %q", key)
				}

				CheckOffset("k0", 0, 0)
				CheckOffset("k01a", 0, 0)
				CheckOffset("k02", 0, 0)
				CheckOffset("k03", 0, 0)
				CheckOffset("k04", 10000, 1000)
				CheckOffset("k04a", 210000, 1000)
				CheckOffset("k05", 210000, 1000)
				CheckOffset("k06", 510000, 1000)
				CheckOffset("k07", 510000, 1000)
				CheckOffset("xyz", 610000, 2000)
			})
		})

		Describe("read test", func() {
			Build := func(kv testutil.KeyValue) testutil.DB {
				o := &opt.Options{
					BlockSize:            512,
					BlockRestartInterval: 3,
				}
				buf := &bytes.Buffer{}

				// Building the table.
				tw := NewWriter(buf, o, nil, 0)
				kv.Iterate(func(i int, key, value []byte) {
					Expect(tw.Append(key, value)).ShouldNot(HaveOccurred())
				})
				tw.Close()

				// Opening the table.
				tr, _ := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), storage.FileDesc{}, nil, nil, o)
				return tableWrapper{tr}
			}
			Test := func(kv *testutil.KeyValue, body func(r *Reader)) func() {
				return func() {
					db := Build(*kv)
					if body != nil {
						body(db.(tableWrapper).Reader)
					}
					testutil.KeyValueTesting(nil, *kv, db, nil, nil)
				}
			}

			testutil.AllKeyValueTesting(nil, Build, nil, nil)
			Describe("with one key per block", Test(testutil.KeyValue_Generate(nil, 9, 1, 1, 10, 512, 512), func(r *Reader) {
				It("should have correct blocks number", func() {
					indexBlock, err := r.readBlock(r.indexBH, true)
					Expect(err).To(BeNil())
					Expect(indexBlock.restartsLen).Should(Equal(9))
				})
			}))
		})
	})
})

// TestWriterCloseTwice ensures closing a table writer twice returns its buffer
// to the pool once, so no two later users of the pool can share it.
func TestWriterCloseTwice(t *testing.T) {
	pool := util.NewBufferPool(opt.DefaultBlockSize + 5)
	w := NewWriter(new(bytes.Buffer), &opt.Options{}, pool, 2*opt.DefaultBlockSize)
	if err := w.Append([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// A second Close reports that the writer is closed.
	if err := w.Close(); err == nil {
		t.Error("second close: expected error")
	}
	if s := pool.String(); !strings.Contains(s, " P·1 ") {
		t.Errorf("buffer returned to the pool more than once: %s", s)
	}
}

// TestReaderUnknownCompressionType ensures a block with an unknown compression
// type reads as corrupted, naming the type, when its buffer comes from a pool.
// It covers that error path only. The fixed bug read the type after putting
// the buffer back in the pool, which a single goroutine cannot catch: nothing
// takes the buffer out again in between.
func TestReaderUnknownCompressionType(t *testing.T) {
	var buf bytes.Buffer
	o := &opt.Options{Compression: opt.NoCompression}
	w := NewWriter(&buf, o, nil, 0)
	if err := w.Append([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	// Find the data block through the index block.
	r, err := NewReader(bytes.NewReader(data), int64(len(data)), storage.FileDesc{}, nil, nil, o)
	if err != nil {
		t.Fatal(err)
	}
	index, err := r.readBlock(r.indexBH, true)
	if err != nil {
		t.Fatal(err)
	}
	iter := r.newBlockIter(index, nil, nil, true)
	if !iter.First() {
		t.Fatal("empty index block")
	}
	bh, n := decodeBlockHandle(iter.Value())
	iter.Release()
	if n == 0 {
		t.Fatal("bad data block handle")
	}

	// Give the data block an unknown compression type and a matching
	// checksum, so the type is what gets checked.
	end := bh.offset + bh.length
	data[end] = 0x7f
	binary.LittleEndian.PutUint32(data[end+1:], util.NewCRC(data[bh.offset:end+1]).Value())

	pool := util.NewBufferPool(opt.DefaultBlockSize + 5)
	r, err = NewReader(bytes.NewReader(data), int64(len(data)), storage.FileDesc{}, nil, pool, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get([]byte("k"), nil); !errors.IsCorrupted(err) ||
		!strings.Contains(err.Error(), "unknown compression type 0x7f") {

		t.Fatalf("get = %v, want a corruption error for compression type 0x7f", err)
	}
}
