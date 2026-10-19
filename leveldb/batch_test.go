// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/gcash/goleveldb/leveldb/errors"
	"github.com/gcash/goleveldb/leveldb/memdb"
	"github.com/gcash/goleveldb/leveldb/testutil"
)

func TestBatchHeader(t *testing.T) {
	f := func(seq uint64, length uint32) bool {
		encoded := encodeBatchHeader(nil, seq, int(length))
		decSeq, decLength, err := decodeBatchHeader(encoded)
		if int(length) < 0 {
			// A 32-bit int cannot hold the count, which decoding rejects.
			return err != nil
		}
		return err == nil && decSeq == seq && decLength == int(length)
	}
	config := &quick.Config{
		Rand: testutil.NewRand(),
	}
	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

type batchKV struct {
	kt   keyType
	k, v []byte
}

func TestBatch(t *testing.T) {
	var (
		kvs         []batchKV
		internalLen int
	)
	batch := new(Batch)
	rbatch := new(Batch)
	abatch := new(Batch)
	testBatch := func(i int, kt keyType, k, v []byte) error {
		kv := kvs[i]
		if kv.kt != kt {
			return fmt.Errorf("invalid key type, index=%d: %d vs %d", i, kv.kt, kt)
		}
		if !bytes.Equal(kv.k, k) {
			return fmt.Errorf("invalid key, index=%d", i)
		}
		if !bytes.Equal(kv.v, v) {
			return fmt.Errorf("invalid value, index=%d", i)
		}
		return nil
	}
	f := func(ktr uint8, k, v []byte) bool {
		kt := keyType(ktr % 2)
		if kt == keyTypeVal {
			batch.Put(k, v)
			rbatch.Put(k, v)
			kvs = append(kvs, batchKV{kt: kt, k: k, v: v})
			internalLen += len(k) + len(v) + 8
		} else {
			batch.Delete(k)
			rbatch.Delete(k)
			kvs = append(kvs, batchKV{kt: kt, k: k})
			internalLen += len(k) + 8
		}
		if batch.Len() != len(kvs) {
			t.Logf("batch.Len: %d vs %d", len(kvs), batch.Len())
			return false
		}
		if batch.internalLen != internalLen {
			t.Logf("abatch.internalLen: %d vs %d", internalLen, batch.internalLen)
			return false
		}
		if len(kvs)%1000 == 0 {
			if err := batch.replayInternal(testBatch); err != nil {
				t.Logf("batch.replayInternal: %v", err)
				return false
			}

			abatch.append(rbatch)
			rbatch.Reset()
			if abatch.Len() != len(kvs) {
				t.Logf("abatch.Len: %d vs %d", len(kvs), abatch.Len())
				return false
			}
			if abatch.internalLen != internalLen {
				t.Logf("abatch.internalLen: %d vs %d", internalLen, abatch.internalLen)
				return false
			}
			if err := abatch.replayInternal(testBatch); err != nil {
				t.Logf("abatch.replayInternal: %v", err)
				return false
			}

			nbatch := new(Batch)
			if err := nbatch.Load(batch.Dump()); err != nil {
				t.Logf("nbatch.Load: %v", err)
				return false
			}
			if nbatch.Len() != len(kvs) {
				t.Logf("nbatch.Len: %d vs %d", len(kvs), nbatch.Len())
				return false
			}
			if nbatch.internalLen != internalLen {
				t.Logf("nbatch.internalLen: %d vs %d", internalLen, nbatch.internalLen)
				return false
			}
			if err := nbatch.replayInternal(testBatch); err != nil {
				t.Logf("nbatch.replayInternal: %v", err)
				return false
			}
		}
		if len(kvs)%10000 == 0 {
			nbatch := new(Batch)
			if err := batch.Replay(nbatch); err != nil {
				t.Logf("batch.Replay: %v", err)
				return false
			}
			if nbatch.Len() != len(kvs) {
				t.Logf("nbatch.Len: %d vs %d", len(kvs), nbatch.Len())
				return false
			}
			if nbatch.internalLen != internalLen {
				t.Logf("nbatch.internalLen: %d vs %d", internalLen, nbatch.internalLen)
				return false
			}
			if err := nbatch.replayInternal(testBatch); err != nil {
				t.Logf("nbatch.replayInternal: %v", err)
				return false
			}
		}
		return true
	}
	config := &quick.Config{
		MaxCount: 40000,
		Rand:     testutil.NewRand(),
	}
	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
	t.Logf("length=%d internalLen=%d", len(kvs), internalLen)
}

func BenchmarkDefaultBatchWrite(b *testing.B) {
	benchmarkBatchWrite(b, nil)
}

func BenchmarkFastAllocationBatchWrite(b *testing.B) {
	benchmarkBatchWrite(b, &BatchConfig{
		GrowLimit: 10 * batchGrowLimit,
	})
}

func benchmarkBatchWrite(b *testing.B, config *BatchConfig) {
	var (
		keys [][]byte
		vals [][]byte
		r    = rand.New(rand.NewSource(1337))
	)
	for i := 0; i < 50000; i++ {
		keys = append(keys, randomString(r, 32))
		vals = append(vals, randomString(r, 100))
	}
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		batch := MakeBatchWithConfig(config)
		for i := 0; i < len(keys); i++ {
			batch.Put(keys[i], vals[i])
		}
	}
	b.ReportAllocs()
}

func TestDecodeBatchToMemRejectsSequencePastInternalKey(t *testing.T) {
	put := func(seq uint64, keys ...string) []byte {
		b := new(Batch)
		for _, key := range keys {
			b.Put([]byte(key), []byte("v"))
		}
		return append(encodeBatchHeader(nil, seq, b.Len()), b.data...)
	}

	for _, test := range []struct {
		name string
		seq  uint64
		keys []string
	}{
		{"header sequence", keyMaxSeq + 1, []string{"k"}},
		{"second record", keyMaxSeq, []string{"a", "b"}},
		// Recovery sets the DB's sequence to the batch's plus its length.
		{"sequence after recovery", keyMaxSeq, []string{"k"}},
	} {
		mdb := memdb.New(defaultIComparer, 1<<20)
		if _, _, err := decodeBatchToMem(put(test.seq, test.keys...), mdb); !errors.IsCorrupted(err) {
			t.Errorf("%s: err = %v", test.name, err)
		}
		// The whole batch is rejected, not only the records past the end.
		if n := mdb.Len(); n != 0 {
			t.Errorf("%s: %d records inserted", test.name, n)
		}
	}

	for _, wantSeq := range []uint64{7, keyMaxSeq - 1} {
		mdb := memdb.New(defaultIComparer, 1<<20)
		seq, n, err := decodeBatchToMem(put(wantSeq, "k"), mdb)
		if err != nil || seq != wantSeq || n != 1 || mdb.Len() != 1 {
			t.Errorf("seq %d: seq=%d n=%d len=%d err=%v", wantSeq, seq, n,
				mdb.Len(), err)
		}
	}
}

// TestBatchLoadRejectsLengthOverflow ensures Load rejects, as corrupt, a
// record whose key or value length does not fit in what is left of the batch,
// however large. The length check converted the length to an int first, so a
// length of 2^63 or more turned negative, passed it, and moved the decoder
// backwards: decoding then panicked, accepted the batch, or looped until
// memory ran out.
func TestBatchLoadRejectsLengthOverflow(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		// A delete whose key length is 2^63; decoding panicked.
		{"key", []byte{0x00, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
		// A key length of 2^64-1, -1 as an int, re-read the length's last
		// byte as a second record, and the batch was accepted.
		{"key_minus_1", []byte{0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00}},
		// A key length of 2^64-11, -11 as an int, went back to the record's
		// start, so decoding looped.
		{"key_minus_11", []byte{0x00, 0xf5, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}},
		// A put of "k" whose value length is 2^63; decoding panicked.
		{"value", []byte{0x01, 0x01, 'k', 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := new(Batch).Load(test.data); !errors.IsCorrupted(err) {
				t.Errorf("Load = %v, want a corruption error", err)
			}
		})
	}
}
