// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package memdb

import (
	"bytes"
	"math/rand"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/gcash/goleveldb/leveldb/comparer"
	"github.com/gcash/goleveldb/leveldb/iterator"
	"github.com/gcash/goleveldb/leveldb/testutil"
	"github.com/gcash/goleveldb/leveldb/util"
)

func (p *DB) TestFindLT(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	if node := p.findLT(key); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

func (p *DB) TestFindLast() (rkey, value []byte, err error) {
	p.mu.RLock()
	if node := p.findLast(); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

func (p *DB) TestPut(key []byte, value []byte) error {
	return p.Put(key, value)
}

func (p *DB) TestDelete(key []byte) error {
	return p.Delete(key)
}

func (p *DB) TestFind(key []byte) (rkey, rvalue []byte, err error) {
	return p.Find(key)
}

func (p *DB) TestGet(key []byte) (value []byte, err error) {
	return p.Get(key)
}

func (p *DB) TestNewIterator(slice *util.Range) iterator.Iterator {
	return p.NewIterator(slice)
}

var _ = testutil.Defer(func() {
	Describe("Memdb", func() {
		Describe("write test", func() {
			It("should do write correctly", func() {
				db := New(comparer.DefaultComparer, 0)
				t := testutil.DBTesting{
					DB:      db,
					Deleted: testutil.KeyValue_Generate(nil, 1000, 1, 1, 30, 5, 5).Clone(),
					PostFn: func(t *testutil.DBTesting) {
						Expect(db.Len()).Should(Equal(t.Present.Len()))
						Expect(db.Size()).Should(Equal(t.Present.Size()))
						switch t.Act {
						case testutil.DBPut, testutil.DBOverwrite:
							Expect(db.Contains(t.ActKey)).Should(BeTrue())
						default:
							Expect(db.Contains(t.ActKey)).Should(BeFalse())
						}
					},
				}
				testutil.DoDBTesting(&t)
			})
		})

		Describe("read test", func() {
			testutil.AllKeyValueTesting(nil, func(kv testutil.KeyValue) testutil.DB {
				// Building the DB.
				db := New(comparer.DefaultComparer, 0)
				kv.IterateShuffled(nil, func(i int, key, value []byte) {
					Expect(db.Put(key, value)).ShouldNot(HaveOccurred())
				})

				if kv.Len() > 1 {
					It("Should find correct keys with findLT", func() {
						testutil.ShuffledIndex(nil, kv.Len()-1, 1, func(i int) {
							key_, key, _ := kv.IndexInexact(i + 1)
							expectedKey, expectedValue := kv.Index(i)

							// Using key that exist.
							rkey, rvalue, err := db.TestFindLT(key)
							Expect(err).ShouldNot(HaveOccurred(), "Error for key %q -> %q", key, expectedKey)
							Expect(rkey).Should(Equal(expectedKey), "Key")
							Expect(rvalue).Should(Equal(expectedValue), "Value for key %q -> %q", key, expectedKey)

							// Using key that doesn't exist.
							rkey, rvalue, err = db.TestFindLT(key_)
							Expect(err).ShouldNot(HaveOccurred(), "Error for key %q (%q) -> %q", key_, key, expectedKey)
							Expect(rkey).Should(Equal(expectedKey))
							Expect(rvalue).Should(Equal(expectedValue), "Value for key %q (%q) -> %q", key_, key, expectedKey)
						})
					})
				}

				if kv.Len() > 0 {
					It("Should find last key with findLast", func() {
						key, value := kv.Index(kv.Len() - 1)
						rkey, rvalue, err := db.TestFindLast()
						Expect(err).ShouldNot(HaveOccurred())
						Expect(rkey).Should(Equal(key))
						Expect(rvalue).Should(Equal(value))
					})
				}

				return db
			}, nil, nil)
		})
	})
})

// TestFindGE checks findGE against a reference that walks each level of the
// skiplist from the head, over keys that share prefixes and keys that are
// prefixes of others, with and without recording the predecessors. It then
// deletes every node taller than one level, which findGE must find exactly.
func TestFindGE(t *testing.T) {
	p := New(comparer.DefaultComparer, 0)
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		key := make([]byte, rnd.Intn(17))
		for j := range key {
			key[j] = "abc"[rnd.Intn(3)]
		}
		if err := p.Put(key, nil); err != nil {
			t.Fatal(err)
		}
	}

	keyOf := func(node int) []byte {
		o := p.nodeData[node]
		return p.kvData[o : o+p.nodeData[node+nKey]]
	}
	var keys [][]byte
	for node := p.nodeData[nNext]; node != 0; node = p.nodeData[node+nNext] {
		keys = append(keys, append([]byte(nil), keyOf(node)...))
	}

	check := func(probe []byte) {
		t.Helper()
		// The last node on each level with a key below the probe.
		var wantPrev [tMaxHeight]int
		for h := 0; h < p.maxHeight; h++ {
			node := 0
			for {
				next := p.nodeData[node+nNext+h]
				if next == 0 || bytes.Compare(keyOf(next), probe) >= 0 {
					break
				}
				node = next
			}
			wantPrev[h] = node
		}
		wantNext := p.nodeData[wantPrev[0]+nNext]
		wantExact := wantNext != 0 && bytes.Equal(keyOf(wantNext), probe)

		if next, exact := p.findGE(probe, false); next != wantNext || exact != wantExact {
			t.Fatalf("findGE(%q, false) = %d, %v; want %d, %v",
				probe, next, exact, wantNext, wantExact)
		}
		p.mu.Lock()
		next, exact := p.findGE(probe, true)
		prev := p.prevNode
		p.mu.Unlock()
		if next != wantNext || exact != wantExact {
			t.Fatalf("findGE(%q, true) = %d, %v; want %d, %v",
				probe, next, exact, wantNext, wantExact)
		}
		for h := 0; h < p.maxHeight; h++ {
			if prev[h] != wantPrev[h] {
				t.Fatalf("findGE(%q, true) recorded node %d at level %d, want %d",
					probe, prev[h], h, wantPrev[h])
			}
		}
	}
	check(nil)
	check([]byte("d"))
	for _, key := range keys {
		check(key)
		check(append(append([]byte(nil), key...), 0))
		if len(key) > 0 {
			check(key[:len(key)-1])
		}
	}

	var tall [][]byte
	for node := p.nodeData[nNext]; node != 0; node = p.nodeData[node+nNext] {
		if p.nodeData[node+nHeight] > 1 {
			tall = append(tall, append([]byte(nil), keyOf(node)...))
		}
	}
	if len(tall) == 0 {
		t.Fatal("no node is taller than one level")
	}
	for _, key := range tall {
		if err := p.Delete(key); err != nil {
			t.Fatalf("delete %q: %v", key, err)
		}
	}
	if want := len(keys) - len(tall); p.Len() != want {
		t.Fatalf("%d entries after deleting the tall nodes, want %d", p.Len(), want)
	}

	// Deleting relinks nodes on every level, which Len does not see. findGE
	// must still agree with a walk of the links, for every key.
	check(nil)
	for _, key := range keys {
		check(key)
	}
}

// TestPutOverwrite ensures putting a key that is already in the DB replaces
// its value, whatever the new value's length, without adding an entry.
func TestPutOverwrite(t *testing.T) {
	p := New(comparer.DefaultComparer, 0)
	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		if err := p.Put([]byte(k), []byte("vv")); err != nil {
			t.Fatal(err)
		}
	}
	// The same length, shorter, longer and empty.
	for _, v := range []string{"xx", "x", "xxxx", ""} {
		for _, k := range keys {
			if err := p.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
		if p.Len() != len(keys) {
			t.Fatalf("value %q: %d entries, want %d", v, p.Len(), len(keys))
		}
		if want := len(keys) * (1 + len(v)); p.Size() != want {
			t.Fatalf("value %q: size %d, want %d", v, p.Size(), want)
		}
		for _, k := range keys {
			if got, err := p.Get([]byte(k)); err != nil || string(got) != v {
				t.Fatalf("get %q = %q, %v; want %q", k, got, err, v)
			}
		}
	}
}
