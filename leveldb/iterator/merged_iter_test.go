// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/gcash/goleveldb/leveldb/comparer"
	. "github.com/gcash/goleveldb/leveldb/iterator"
	"github.com/gcash/goleveldb/leveldb/testutil"
)

var _ = testutil.Defer(func() {
	Describe("Merged iterator", func() {
		Test := func(filled int, empty int) func() {
			return func() {
				It("Should iterates and seeks correctly", func(done Done) {
					rnd := testutil.NewRand()

					// Build key/value.
					filledKV := make([]testutil.KeyValue, filled)
					kv := testutil.KeyValue_Generate(nil, 100, 1, 1, 10, 4, 4)
					kv.Iterate(func(i int, key, value []byte) {
						filledKV[rnd.Intn(filled)].Put(key, value)
					})

					// Create itearators.
					iters := make([]Iterator, filled+empty)
					for i := range iters {
						if empty == 0 || (rnd.Int()%2 == 0 && filled > 0) {
							filled--
							Expect(filledKV[filled].Len()).ShouldNot(BeZero())
							iters[i] = NewArrayIterator(filledKV[filled])
						} else {
							empty--
							iters[i] = NewEmptyIterator(nil)
						}
					}

					// Test the iterator.
					t := testutil.IteratorTesting{
						KeyValue: kv.Clone(),
						Iter:     NewMergedIterator(iters, comparer.DefaultComparer, true),
					}
					testutil.DoIteratorTesting(&t)
					done <- true
				}, 15.0)
			}
		}

		Describe("with three, all filled iterators", Test(3, 0))
		Describe("with one filled, one empty iterators", Test(1, 1))
		Describe("with one filled, two empty iterators", Test(1, 2))
	})
})

// TestMergedIteratorRandomWalk checks a merged iterator against a sorted
// reference over random sequences of First, Last, Seek, Next and Prev, so
// that it changes direction anywhere, not only at the ends.
func TestMergedIteratorRandomWalk(t *testing.T) {
	var keys []string
	for i := 0; i < 200; i++ {
		// Odd numbers, so even probes fall between keys.
		keys = append(keys, fmt.Sprintf("k%04d", 2*i+1))
	}

	for seed := int64(0); seed < 20; seed++ {
		for _, strict := range []bool{true, false} {
			rnd := rand.New(rand.NewSource(seed))

			// Spread the keys over a few sub-iterators and add some empty ones.
			subs := make([]testutil.KeyValue, 1+rnd.Intn(6))
			for _, k := range keys {
				subs[rnd.Intn(len(subs))].PutString(k, "v"+k)
			}
			var iters []Iterator
			for _, kv := range subs {
				iters = append(iters, NewArrayIterator(kv))
			}
			for e := rnd.Intn(3); e > 0; e-- {
				iters = append(iters, NewEmptyIterator(nil))
			}
			rnd.Shuffle(len(iters), func(i, j int) {
				iters[i], iters[j] = iters[j], iters[i]
			})
			mi := NewMergedIterator(iters, comparer.DefaultComparer, strict)

			// -1 is before the first key and len(keys) after the last.
			pos := -1
			for op := 0; op < 2000; op++ {
				var name string
				var got bool
				switch rnd.Intn(8) {
				case 0:
					name, got = "First", mi.First()
					pos = 0
				case 1:
					name, got = "Last", mi.Last()
					pos = len(keys) - 1
				case 2:
					probe := fmt.Sprintf("k%04d", rnd.Intn(2*len(keys)+2))
					name, got = "Seek "+probe, mi.Seek([]byte(probe))
					pos = sort.SearchStrings(keys, probe)
				case 3, 4, 5:
					name, got = "Next", mi.Next()
					if pos < len(keys) {
						pos++
					}
				default:
					name, got = "Prev", mi.Prev()
					if pos >= 0 {
						pos--
					}
				}

				want := pos >= 0 && pos < len(keys)
				if got != want {
					t.Fatalf("seed %d strict %v op %d %s: got %v, want %v",
						seed, strict, op, name, got, want)
				}
				if !want {
					continue
				}
				if k := string(mi.Key()); k != keys[pos] {
					t.Fatalf("seed %d strict %v op %d %s: key %q, want %q",
						seed, strict, op, name, k, keys[pos])
				}
				if v := string(mi.Value()); v != "v"+keys[pos] {
					t.Fatalf("seed %d strict %v op %d %s: value %q, want %q",
						seed, strict, op, name, v, "v"+keys[pos])
				}
			}
			mi.Release()
		}
	}
}

func benchmarkMergedIteratorN(b *testing.B, n int) {
	iters := make([]Iterator, n)
	for i := range iters {
		kv := testutil.KeyValue_Generate(nil, 100, 1, 1, 10, 4, 4)
		iters[i] = NewArrayIterator(kv)
	}

	mi := NewMergedIterator(iters, comparer.DefaultComparer, true)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mi.First()
		for mi.Next() {
			mi.Key()
		}
	}
}

func BenchmarkMergedIterator(b *testing.B) {
	b.Run("2 iters", func(b *testing.B) {
		benchmarkMergedIteratorN(b, 2)
	})

	b.Run("50 iters", func(b *testing.B) {
		benchmarkMergedIteratorN(b, 50)
	})
}
