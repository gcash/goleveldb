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

// journalSyncError reports a failed journal sync. The journal's state is
// unknown after one, so the DB stops all writes until it is reopened.
type journalSyncError struct{ err error }

func (e *journalSyncError) Error() string {
	return "leveldb: journal sync failed, writes stopped until reopened: " + e.err.Error()
}

func (e *journalSyncError) Unwrap() error { return e.err }
