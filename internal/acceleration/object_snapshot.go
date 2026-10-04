// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

// withObjectSnapshotProvenance binds the already minted capabilities to their
// acquired generation. It copies all mutable envelope state; provider keys and
// opaque object versions stay exclusively in the parent-owned range bridge.
func withObjectSnapshotProvenance(source catalog.Source, snapshot Snapshot, dataset catalog.Dataset) (catalog.Source, error) {
	bad := errors.New("object snapshot provenance is unavailable")
	if dataset.ID != snapshot.Dataset || source.ID != snapshot.Dataset || source.ObjectSnapshot != nil {
		return catalog.Source{}, bad
	}
	scan, err := dataset.EffectiveSnapshotScanLimits()
	if err != nil {
		return catalog.Source{}, bad
	}
	read := &catalog.ObjectSnapshotRead{Dataset: snapshot.Dataset, Generation: snapshot.Generation,
		SchemaSHA256: snapshot.SchemaHash, Scan: scan}
	if len(snapshot.Parts) == 0 {
		if source.Range == nil || source.Ranges != nil {
			return catalog.Source{}, bad
		}
		capability := *source.Range
		source.Range = &capability
		read.Parts = []catalog.ObjectSnapshotPart{{URL: capability.URL, Rows: snapshot.Rows, Bytes: snapshot.Bytes, SHA256: snapshot.SHA256}}
	} else {
		if len(snapshot.Parts) > 256 || len(snapshot.Parts) != len(source.Ranges) || source.Range != nil {
			return catalog.Source{}, bad
		}
		source.Ranges = append([]catalog.ObjectRange(nil), source.Ranges...)
		read.Parts = make([]catalog.ObjectSnapshotPart, len(snapshot.Parts))
		var rows, bytes int64
		for index, part := range snapshot.Parts {
			if part.Rows < 0 || part.Rows > snapshot.Rows-rows || part.Bytes < 12 || part.Bytes > snapshot.Bytes-bytes {
				return catalog.Source{}, bad
			}
			rows += part.Rows
			bytes += part.Bytes
			read.Parts[index] = catalog.ObjectSnapshotPart{URL: source.Ranges[index].URL, Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256}
		}
		if rows != snapshot.Rows || bytes != snapshot.Bytes {
			return catalog.Source{}, bad
		}
	}
	source.ObjectSnapshot = read
	if err := source.ValidateObjectSnapshot(); err != nil {
		return catalog.Source{}, bad
	}
	return source, nil
}
