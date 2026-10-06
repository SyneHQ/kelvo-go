// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type OperationFileResolver func(context.Context, adapter.ProcessRequest, io.Writer) (int64, error)
type operationFileResolverKey struct{}

// WithOperationFileResolver binds the authenticated operation to private file
// delivery. Public requests cannot choose a callback, path, URL or descriptor.
func WithOperationFileResolver(ctx context.Context, resolve OperationFileResolver) context.Context {
	return context.WithValue(ctx, operationFileResolverKey{}, resolve)
}

func operationFileDescriptor(source catalog.Source, secrets map[string]string) (*filesnapshot.Descriptor, error) {
	if source.ID != "source_1" || !filesnapshot.FormatSupported(source.Type) || source.Path != "" || source.Adapter != "" || source.DSNEnv != "" || source.URLEnv != "" || source.UsernameEnv != "" || source.PasswordEnv != "" || source.TokenEnv != "" || source.Federation != nil || source.LocalSnapshot != nil || source.ObjectSnapshot != nil || source.ParquetPaths != nil || source.Ranges != nil || source.Object != nil || source.Range != nil || len(secrets) != 0 || len(source.Options) != 3 {
		return nil, connectionUnavailable()
	}
	n, err := strconv.ParseInt(source.Options["file_bytes"], 10, 64)
	d := &filesnapshot.Descriptor{Version: filesnapshot.Version, Format: source.Options["file_format"], Bytes: n, SHA256: source.Options["file_sha256"]}
	if err != nil || strconv.FormatInt(n, 10) != source.Options["file_bytes"] || d.Format != source.Type || d.Validate() != nil {
		return nil, connectionUnavailable()
	}
	return d, nil
}

// FetchOperationFile fetches only the snapshot declared by the admitted source
// resolution. A final trailer proves authorization was rechecked after delivery.
func (e *Executor) FetchOperationFile(ctx context.Context, record operationstore.Record, request operations.Request, revision string, snapshot filesnapshot.Descriptor, destination io.Writer) (int64, error) {
	if ctx == nil || e == nil || destination == nil || snapshot.Validate() != nil || !operations.ValidDigest(revision) || request.Validate() != nil || (request.Kind.Mutating() && (request.Kind != operations.StatementExecute || (snapshot.Format != "sqlite" && snapshot.Format != "duckdb"))) || record.State != operationstore.Running || record.Binding.Validate() != nil || record.Scope.Validate() != nil || request.Connection.ID != record.Scope.ConnectionID || request.Kind != record.Kind || record.AuthoritySHA256 != operations.GrantDigest(record.AuthorityToken) {
		return 0, connectionUnavailable()
	}
	digest, err := operations.Digest(request)
	if err != nil || digest != record.RequestSHA256 {
		return 0, connectionUnavailable()
	}
	ctx, cancel := context.WithDeadline(ctx, record.ExecuteBefore)
	defer cancel()
	resolver := e.connectionResolvers[record.Scope.Issuer]
	if resolver == nil || resolver.client == nil || resolver.slots == nil || !strings.HasSuffix(resolver.url, "/internal/kelvo/resolve") {
		return 0, connectionUnavailable()
	}
	select {
	case resolver.slots <- struct{}{}:
		defer func() { <-resolver.slots }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	body := struct {
		operationResolutionRequest
		SourceRevision string                  `json:"source_revision"`
		Snapshot       filesnapshot.Descriptor `json:"snapshot"`
	}{operationResolutionRequest{Grant: record.AuthorityToken, Operation: request, OperationID: record.ID, WorkerID: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim}, revision, snapshot}
	raw, err := json.Marshal(body)
	if err != nil || len(raw) > operations.MaxRequestBytes+operations.MaxGrantBytes+8192 {
		return 0, connectionUnavailable()
	}
	defer clear(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(resolver.url, "/resolve")+"/operation-file", bytes.NewReader(raw))
	if err != nil {
		return 0, connectionUnavailable()
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	// The query deadline bounds the stream; keep the resolver's existing TLS
	// transport, admission and header timeout without its short JSON-body timer.
	client := *resolver.client
	client.Timeout = 0
	resp, err := client.Do(req)
	if err != nil {
		return 0, connectionUnavailable()
	}
	defer resp.Body.Close()
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/octet-stream" || resp.StatusCode != http.StatusOK || resp.ContentLength != -1 || resp.Header.Get("Content-Encoding") != "" || len(resp.Header.Values("X-Kelvo-File-SHA256")) != 1 || resp.Header.Get("X-Kelvo-File-SHA256") != snapshot.SHA256 || resp.Header.Get("X-Kelvo-File-Verified") != "" || resp.Header.Get("X-Kelvo-Source-Valid-Until") != "" {
		return 0, connectionUnavailable()
	}
	if _, ok := resp.Trailer[http.CanonicalHeaderKey("X-Kelvo-File-Verified")]; !ok {
		return 0, connectionUnavailable()
	}
	if _, ok := resp.Trailer[http.CanonicalHeaderKey("X-Kelvo-Source-Valid-Until")]; !ok {
		return 0, connectionUnavailable()
	}
	hash := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(destination, hash), io.LimitReader(resp.Body, snapshot.Bytes+1), make([]byte, 64<<10))
	if err != nil || n != snapshot.Bytes || hex.EncodeToString(hash.Sum(nil)) != snapshot.SHA256 || ctx.Err() != nil {
		return 0, connectionUnavailable()
	}
	now := time.Now()
	peerUntil, ok := resolverCertificateExpiry(resp.TLS, now)
	until, err := strconv.ParseInt(resp.Trailer.Get("X-Kelvo-Source-Valid-Until"), 10, 64)
	if !ok || err != nil || len(resp.Trailer.Values("X-Kelvo-File-Verified")) != 1 || len(resp.Trailer.Values("X-Kelvo-Source-Valid-Until")) != 1 || resp.Trailer.Get("X-Kelvo-File-Verified") != snapshot.SHA256 || strconv.FormatInt(until, 10) != resp.Trailer.Get("X-Kelvo-Source-Valid-Until") || until <= now.Unix() || until > now.Unix()+5 || until > record.AuthorityUntil.Unix() {
		return 0, connectionUnavailable()
	}
	until = min(until, peerUntil.Unix(), record.ExecuteBefore.Unix())
	if until <= now.Unix() {
		return 0, connectionUnavailable()
	}
	return until, nil
}
