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
)

// writesStoppedError reports a failure that leaves what is on disk unknown, a
// journal sync or a MANIFEST commit, after which the DB stops all writes until
// it is reopened.
type writesStoppedError struct {
	what string
	err  error
}

func (e *writesStoppedError) Error() string {
	return "leveldb: " + e.what + " failed, writes stopped until reopened: " + e.err.Error()
}

func (e *writesStoppedError) Unwrap() error { return e.err }
