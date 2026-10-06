package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type OperationFilePublisher func(context.Context, adapter.ProcessRequest, filesnapshot.Descriptor, io.Reader) (bool, error)
type operationFilePublisherKey struct{}

func WithOperationFilePublisher(ctx context.Context, publish OperationFilePublisher) context.Context {
	return context.WithValue(ctx, operationFilePublisherKey{}, publish)
}

func (e *Executor) PublishOperationFile(ctx context.Context, record operationstore.Record, request operations.Request, revision string, p filesnapshot.Publication, source io.Reader) (bool, error) {
	if ctx == nil || e == nil || source == nil || p.Validate() != nil || !operations.ValidDigest(revision) || request.Validate() != nil || request.Kind != operations.StatementExecute || record.State != operationstore.Running || record.Binding.Validate() != nil || record.Scope.Validate() != nil || request.Connection.ID != record.Scope.ConnectionID || request.Kind != record.Kind || record.AuthoritySHA256 != operations.GrantDigest(record.AuthorityToken) || p.OperationID != record.ID || p.RequestSHA256 != record.RequestSHA256 {
		return false, connectionUnavailable()
	}
	digest, err := operations.Digest(request)
	if err != nil || digest != record.RequestSHA256 {
		return false, connectionUnavailable()
	}
	ctx, cancel := context.WithDeadline(ctx, record.ExecuteBefore)
	defer cancel()
	resolver := e.connectionResolvers[record.Scope.Issuer]
	if resolver == nil || resolver.client == nil || resolver.slots == nil || !strings.HasSuffix(resolver.url, "/internal/kelvo/resolve") {
		return false, connectionUnavailable()
	}
	select {
	case resolver.slots <- struct{}{}:
		defer func() { <-resolver.slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	body := struct {
		operationResolutionRequest
		SourceRevision string                   `json:"source_revision"`
		Snapshot       filesnapshot.Descriptor  `json:"snapshot"`
		Publication    filesnapshot.Publication `json:"publication"`
	}{operationResolutionRequest{Grant: record.AuthorityToken, Operation: request, OperationID: record.ID, WorkerID: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim}, revision, p.Original, p}
	raw, err := json.Marshal(body)
	if err != nil || len(raw) > operations.MaxRequestBytes+operations.MaxGrantBytes+4096 {
		return false, connectionUnavailable()
	}
	defer clear(raw)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(raw)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(resolver.url, "/resolve")+"/operation-file-commit", io.MultiReader(bytes.NewReader(size[:]), bytes.NewReader(raw), source))
	if err != nil {
		return false, connectionUnavailable()
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/vnd.kelvo.file-update")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	client := *resolver.client
	client.Timeout = 0
	response, err := client.Do(req)
	if err != nil {
		return false, connectionUnavailable()
	}
	defer response.Body.Close()
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusConflict || response.Header.Get("Content-Encoding") != "" {
		return false, connectionUnavailable()
	}
	if _, ok := resolverCertificateExpiry(response.TLS, time.Now()); !ok {
		return false, connectionUnavailable()
	}
	result, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || len(result) > 8192 {
		return false, connectionUnavailable()
	}
	var receipt filesnapshot.PublicationReceipt
	if operations.DecodeStrict(result, &receipt, 8192) != nil || !receipt.Matches(p) || receipt.Committed != (response.StatusCode == http.StatusOK) {
		return false, connectionUnavailable()
	}
	return receipt.Committed, nil
}
