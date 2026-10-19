// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/gcash/goleveldb/leveldb/errors"
)

// Common errors.
var (
	ErrNotFound         = errors.ErrNotFound
	ErrReadOnly         = errors.New("leveldb: read-only mode")
	ErrSnapshotReleased = errors.New("leveldb: snapshot released")
	ErrIterReleased     = errors.New("leveldb: iterator released")
	ErrClosed           = errors.New("leveldb: closed")

	// ErrWritesStopped is matched, through errors.Is, by the error writes,
	// transactions, compactions, SetReadOnly and Close return once a journal
	// write, sync or rotation, or a MANIFEST commit, has failed. What reached
	// the disk is then unknown, so the DB takes no more writes until it is
	// reopened. LevelDB stops the same way, except after a failed journal
	// write, which it only returns to the writes involved. Reads keep working.
	ErrWritesStopped = errors.New("leveldb: writes stopped until reopened")
)

// errSeqExhausted is returned by a write that would pass the largest
// sequence number the DB can use.
var errSeqExhausted = errors.New("leveldb: sequence numbers exhausted")

// writesStoppedError reports a failure that leaves what is on disk unknown, of
// a journal write, sync or rotation, or a MANIFEST commit, after which the DB
// stops all writes until it is reopened.
type writesStoppedError struct {
	what string
	err  error
}

func (e *writesStoppedError) Error() string {
	return "leveldb: " + e.what + " failed, writes stopped until reopened: " + e.err.Error()
}

func (e *writesStoppedError) Unwrap() error { return e.err }

// Is reports whether target is ErrWritesStopped, for errors.Is.
func (e *writesStoppedError) Is(target error) bool { return target == ErrWritesStopped }

func isWritesStopped(err error) bool {
	_, ok := err.(*writesStoppedError)
	return ok
}
