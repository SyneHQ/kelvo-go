//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"context"
	"os"
	"time"
)

// ReadPage is operator-only and offline: an active writer returns ErrBusy.
// Filesystem ownership is its authorization boundary. It scans at most limit
// physical slots (1..256), not unbounded history, and never modifies recovery
// state. Stop/drain the service before reading; no tenant HTTP API uses it.
func ReadPage(ctx context.Context, directory string, cursor, limit int) (Page, error) {
	page := Page{Events: []Event{}}
	if cursor < 0 || cursor > maximumEntries || limit < 1 || limit > maximumPage {
		return page, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	dir, err := openDirectory(directory, false)
	if err != nil {
		return page, err
	}
	defer dir.Close()
	lock, err := lockWriter(dir, true)
	if err != nil {
		return page, err
	}
	defer lock.Close()
	file, err := openChild(dir, journalName, os.O_RDONLY)
	if err != nil {
		return page, ErrUnavailable
	}
	defer file.Close()
	rawHeader := make([]byte, headerSize)
	if readExact(file, rawHeader, 0) != nil {
		return page, ErrCorrupt
	}
	header, err := decodeHeader(rawHeader, directory)
	if err != nil {
		return page, err
	}
	st, err := checkFile(file, false)
	if err != nil || st.Size != int64(headerSize)+int64(header.Config.MaxEntries)*slotSize || cursor > header.Config.MaxEntries {
		return page, ErrCorrupt
	}
	end := min(cursor+limit, header.Config.MaxEntries)
	raw := make([]byte, slotSize)
	now := time.Now().UnixNano()
	for slot := cursor; slot < end; slot++ {
		if err := ctx.Err(); err != nil {
			return Page{}, err
		}
		if readExact(file, raw, slotOffset(slot)) != nil {
			return Page{}, ErrCorrupt
		}
		start, finish, err := decodeSlot(raw, header.Scope, header.Config.Retention)
		if err != nil {
			return Page{}, err
		}
		if start.ID == "" || (finish != nil && finish.FinishedAt+int64(header.Config.Retention) <= now) {
			continue
		}
		page.Events = append(page.Events, eventFrom(start, finish))
	}
	page.Next = end
	page.Done = end == header.Config.MaxEntries
	return page, nil
}
