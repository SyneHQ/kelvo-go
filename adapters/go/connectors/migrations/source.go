// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"io"
	"os"
	"sort"
	"strings"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/golang-migrate/migrate/v4/source"
)

// sealedSource implements the library's source interface without filesystem or
// network access. Version ordering and execution remain golang-migrate's job.
type sealedSource struct {
	versions []uint
	up, down map[uint]migration.File
}

var _ source.Driver = (*sealedSource)(nil)

func newSource(files []migration.File) *sealedSource {
	s := &sealedSource{up: map[uint]migration.File{}, down: map[uint]migration.File{}}
	seen := map[uint]bool{}
	for _, f := range files {
		v, d, _ := migration.ParseName(f.Name)
		version := uint(v)
		if !seen[version] {
			s.versions = append(s.versions, version)
			seen[version] = true
		}
		if d == "up" {
			s.up[version] = f
		} else {
			s.down[version] = f
		}
	}
	sort.Slice(s.versions, func(i, j int) bool { return s.versions[i] < s.versions[j] })
	return s
}
func (*sealedSource) Open(string) (source.Driver, error) { return nil, migration.ErrInvalid }
func (*sealedSource) Close() error                       { return nil }
func (s *sealedSource) First() (uint, error) {
	if len(s.versions) == 0 {
		return 0, os.ErrNotExist
	}
	return s.versions[0], nil
}
func (s *sealedSource) Prev(v uint) (uint, error) {
	i := sort.Search(len(s.versions), func(i int) bool { return s.versions[i] >= v })
	if i == 0 {
		return 0, os.ErrNotExist
	}
	return s.versions[i-1], nil
}
func (s *sealedSource) Next(v uint) (uint, error) {
	i := sort.Search(len(s.versions), func(i int) bool { return s.versions[i] > v })
	if i == len(s.versions) {
		return 0, os.ErrNotExist
	}
	return s.versions[i], nil
}
func (s *sealedSource) ReadUp(v uint) (io.ReadCloser, string, error)   { return readFile(s.up, v) }
func (s *sealedSource) ReadDown(v uint) (io.ReadCloser, string, error) { return readFile(s.down, v) }
func readFile(files map[uint]migration.File, v uint) (io.ReadCloser, string, error) {
	f, ok := files[v]
	if !ok {
		return nil, "", os.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(f.Content)), f.Name, nil
}
