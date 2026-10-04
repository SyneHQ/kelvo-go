//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"go.yaml.in/yaml/v3"
)

// Custody pins one persistent export namespace to a tenant and worker. The
// lifetime lock excludes a second runtime, including another process. Its
// private data child keeps custody metadata outside the export store format.
type Custody struct {
	mu            sync.Mutex
	root, lock    *os.File
	id, directory string
}

type custodyRecord struct {
	Version int    `yaml:"version"`
	Tenant  string `yaml:"tenant"`
	Worker  string `yaml:"worker"`
	ID      string `yaml:"id"`
}

func OpenCustody(ctx context.Context, directory, tenant, worker string) (*Custody, error) {
	if !tenantPattern.MatchString(tenant) || !tenantPattern.MatchString(worker) {
		return nil, ErrInvalid
	}
	root, err := openRoot(directory)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = root.Close()
		}
	}()
	lock, err := lockRoot(ctx, root)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !keep {
			_ = lock.Close()
		}
	}()
	names, err := directoryNames(root, 3)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if name != ".lock" && name != "custody.yml" && name != "data" {
			return nil, ErrCorrupt
		}
	}
	var stored custodyRecord
	raw, err := readFile(root, "custody.yml", stateLimit, true)
	if errors.Is(err, os.ErrNotExist) {
		// Missing authority alongside existing data is never an invitation to
		// adopt it, even if the directory happens to be empty.
		for _, name := range names {
			if name != ".lock" {
				return nil, ErrCorrupt
			}
		}
		id, e := randomID()
		if e != nil {
			return nil, e
		}
		stored = custodyRecord{Version: 1, Tenant: tenant, Worker: worker, ID: id}
		raw, err = yaml.Marshal(stored)
		if err == nil {
			published, writeErr := atomicWrite(root, "custody.yml", raw, true, true)
			err = writeErr
			if err != nil && published {
				err = errors.Join(ErrPublicationUncertain, err)
			}
		}
	} else if err == nil {
		err = strictYAML(raw, &stored)
	}
	if err != nil {
		return nil, err
	}
	if stored.Version != 1 || stored.Tenant != tenant || stored.Worker != worker || !idPattern.MatchString(stored.ID) {
		return nil, ErrCorrupt
	}
	data, err := childDir(root, "data", false)
	if errors.Is(err, os.ErrNotExist) {
		data, err = childDir(root, "data", true)
	}
	if err != nil {
		return nil, err
	}
	if err = data.Close(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	keep = true
	return &Custody{root: root, lock: lock, id: stored.ID, directory: filepath.Join(directory, "data")}, nil
}

func (c *Custody) ID() string            { return c.id }
func (c *Custody) DataDirectory() string { return c.directory }

// Close must follow the close of every store/writer/reader using this custody.
// It never removes retained files or changes their identity.
func (c *Custody) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.root == nil {
		return nil
	}
	err := errors.Join(c.lock.Close(), c.root.Close())
	c.lock, c.root = nil, nil
	return err
}
