// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	resolverapi "github.com/SYNEHQ/kelvo-go/resolver"
)

// queryFiles fetches federated file sources resolved for one delegated query.
// It is execution-only state on the resolved executor copy.
type queryFiles struct {
	descriptors map[string]filesnapshot.Descriptor
	fetch       func(context.Context, string, filesnapshot.Descriptor, *os.File) error
}

// fetchQueryFile posts the same delegation, query, job and worker binding as
// the resolve call, plus the alias and descriptor, and writes verified bytes.
func (r *ConnectionResolver) fetchQueryFile(ctx context.Context, execution delegation.Execution, request query.Request, alias string, snapshot filesnapshot.Descriptor, destination *os.File) error {
	if r == nil || r.client == nil || r.slots == nil || !strings.HasSuffix(r.url, resolverapi.QueryPath) {
		return connectionUnavailable()
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	request.Delegation = ""
	body := resolverapi.QueryFileReadRequest{QueryRequest: connectionResolutionRequest{
		Delegation: execution.Token, Query: request, JobID: execution.Binding.JobID,
		WorkerID: execution.Binding.WorkerID, Owner: execution.Binding.Owner, Claim: execution.Binding.Claim,
	}, Alias: alias, Snapshot: snapshot}
	if body.Validate() != nil {
		return connectionUnavailable()
	}
	raw, err := json.Marshal(body)
	if err != nil || len(raw) > resolverapi.MaxQueryRequestBytes+4096 {
		return connectionUnavailable()
	}
	defer clear(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(r.url, resolverapi.QueryPath)+resolverapi.QueryFileReadPath, bytes.NewReader(raw))
	if err != nil {
		return connectionUnavailable()
	}
	_, err = receiveVerifiedFile(ctx, r, req, snapshot, execution.Claims.ExpiresAt, execution.Claims.ExpiresAt, destination)
	return err
}

// spool writes each federated file source into the per-query workspace dir,
// re-hashes it and points the runtime source at the spooled path. The files
// are removed with the workspace, on success or failure.
func (f *queryFiles) spool(ctx context.Context, dir string, sources []catalog.Source) error {
	spooled := 0
	for i := range sources {
		if !sources[i].FederatedFile {
			continue
		}
		if f == nil {
			return connectionUnavailable()
		}
		descriptor, ok := f.descriptors[sources[i].ID]
		if !ok || sources[i].Path != "" || descriptor.Format != sources[i].Type {
			return connectionUnavailable()
		}
		path := filepath.Join(dir, "kelvo-file-"+sources[i].ID+"."+descriptor.Format)
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return query.NewError("RESOURCE_EXHAUSTED", "Worker scratch workspace is unavailable")
		}
		err = f.fetch(ctx, sources[i].ID, descriptor, file)
		if err == nil {
			err = verifySpooled(file, descriptor)
		}
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = connectionUnavailable()
		}
		if err != nil {
			return err
		}
		sources[i].Path = path
		spooled++
	}
	if f != nil && spooled != len(f.descriptors) {
		return connectionUnavailable()
	}
	return nil
}

// verifySpooled re-reads the written file: the stream hash covered what was
// sent, this covers what the query process will read.
func verifySpooled(file *os.File, descriptor filesnapshot.Descriptor) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return connectionUnavailable()
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(file, descriptor.Bytes+1))
	if err != nil || n != descriptor.Bytes || hex.EncodeToString(hash.Sum(nil)) != descriptor.SHA256 {
		return connectionUnavailable()
	}
	return nil
}
