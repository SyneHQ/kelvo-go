// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const ExactValue int64 = 9007199254740993

type ExerciseOptions struct {
	Team, Subject, Connection, RunID, Journal, Mode string
	ExpectDenied                                    bool
	ExpectResolverUnavailable                       bool
}
type ExerciseReport struct {
	Checks               []string            `json:"checks"`
	Failure              *ExerciseFailure    `json:"failure,omitempty"`
	Team                 string              `json:"team"`
	Connection           string              `json:"connection"`
	Mutation             operations.Response `json:"mutation"`
	Read                 operations.Response `json:"read"`
	ReadRows             int64               `json:"read_rows"`
	QueryID              string              `json:"query_id"`
	QueryInitialState    string              `json:"query_initial_state"`
	QueryStateOnFailure  string              `json:"query_state_on_failure,omitempty"`
	QueryErrorCode       string              `json:"query_error_code,omitempty"`
	QueryStatusErrorCode string              `json:"query_status_error_code,omitempty"`
	QueryStats           client.Stats        `json:"query_stats"`
	CancelledQueryID     string              `json:"cancelled_query_id"`
	CancellationState    string              `json:"cancellation_state"`
}

type ExerciseFailure struct {
	Stage string `json:"stage"`
	Code  string `json:"code"`
}

type exerciseStageError struct {
	failure               ExerciseFailure
	queryState, queryCode string
	cause                 error
}

func (e *exerciseStageError) Error() string {
	message := "exercise " + e.failure.Stage + ": " + e.failure.Code
	if e.queryState != "" {
		message += " (query state=" + e.queryState + ", code=" + e.queryCode + ")"
	}
	if e.failure.Stage == "mutation" {
		message += "; preserve the journal and reconcile without resubmitting"
	}
	return message
}
func (e *exerciseStageError) Unwrap() error { return e.cause }

func exerciseErrorCode(err error) string {
	var clientError *client.Error
	var queryError *query.Error
	code := ""
	switch {
	case errors.As(err, &clientError):
		code = clientError.Code
	case errors.As(err, &queryError):
		code = queryError.Code
	case errors.Is(err, context.Canceled):
		code = "CANCELLED"
	case errors.Is(err, context.DeadlineExceeded):
		code = "DEADLINE_EXCEEDED"
	case errors.Is(err, ErrConfiguration):
		code = "INVALID_CONFIG"
	case errors.Is(err, ErrPolicyDenied):
		code = "PERMISSION_DENIED"
	}
	// Never copy arbitrary error strings, remote messages, or unrecognized codes.
	switch code {
	case "INVALID_CONFIG", "CONFIGURATION_ERROR", "INVALID_ARGUMENT", "RESOURCE_EXHAUSTED",
		"PROTOCOL_ERROR", "CANCELLED", "DEADLINE_EXCEEDED", "UNAUTHENTICATED",
		"PERMISSION_DENIED", "QUERY_FAILED", "NOT_FOUND", "CLIENT_CLOSED", "SINK_FAILED", "UNAVAILABLE":
		return code
	default:
		return "FAILED"
	}
}

func queryFailureStatus(ctx context.Context, gateway *client.Client, id string, auth client.Authority, report *ExerciseReport) (client.QueryStatus, error) {
	// A failed Results call may exhaust its context. One independent, bounded
	// control call can still preserve terminal state without replaying any work.
	diagnostic, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	status, err := gateway.Status(diagnostic, id, auth)
	if err != nil {
		report.QueryStatusErrorCode = exerciseErrorCode(err)
		return status, err
	}
	report.QueryStateOnFailure = status.State
	if status.Error != nil {
		report.QueryErrorCode = exerciseErrorCode(status.Error)
	}
	return status, nil
}

func nonce() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func queryGrant(c Config, key ed25519.PrivateKey, o ExerciseOptions, connection Connection, r query.Request) (string, error) {
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		return "", err
	}
	id, err := nonce()
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	i := c.Identity
	return delegation.Sign(delegation.Claims{Version: 1, Issuer: i.Issuer, Audience: i.Audience, ClusterTenant: i.ClusterTenant, ServicePrincipal: i.ServicePrincipal,
		AppTeam: o.Team, Subject: delegation.Subject{Kind: "user", ID: o.Subject}, ID: id, IssuedAt: now, ExpiresAt: now + 240, QuerySHA256: digest,
		Sources: []delegation.Source{{Alias: "source_1", ConnectionID: o.Connection, Database: connection.Database, Schema: connection.Schema}}}, key)
}

func operationGrant(c Config, key ed25519.PrivateKey, o ExerciseOptions, r operations.Request) (string, error) {
	digest, err := operations.Digest(r)
	if err != nil {
		return "", err
	}
	id, err := nonce()
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	i := c.Identity
	authority := "read"
	if r.Kind.Mutating() {
		authority = "trusted_app"
	}
	return operations.SignGrant(operations.GrantClaims{Version: operations.GrantVersion, Issuer: i.Issuer, Audience: i.Audience, ClusterTenant: i.ClusterTenant, ServicePrincipal: i.ServicePrincipal,
		AppTeam: o.Team, Subject: operations.Subject{Kind: "user", ID: o.Subject}, ID: id, IssuedAt: now, ExpiresAt: now + 240, ConnectionID: r.Connection.ID,
		Operation: r.Kind, RequestSHA256: digest, Authorization: operations.Authorization{Kind: authority}}, key)
}

// mutationJournal retains the original grant and exact request before the first
// network call. It is private local application state, never an example fixture.
type mutationJournal struct {
	Version    int                  `json:"version"`
	Team       string               `json:"team"`
	Subject    string               `json:"subject"`
	Connection string               `json:"connection"`
	RunID      string               `json:"run_id"`
	Request    operations.Request   `json:"request"`
	Digest     string               `json:"request_sha256"`
	Grant      string               `json:"grant"`
	ID         string               `json:"operation_id,omitempty"`
	Response   *operations.Response `json:"response,omitempty"`
}

func saveJournal(path string, j mutationJournal, first bool) error {
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	defer clear(raw)
	if first {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return errors.New("mutation journal already exists or is unavailable")
		}
		n, writeErr := f.Write(raw)
		syncErr := f.Sync()
		closeErr := f.Close()
		if n != len(raw) || writeErr != nil || syncErr != nil || closeErr != nil {
			return errors.New("mutation intent could not be persisted")
		}
	} else {
		f, err := os.CreateTemp(filepath.Dir(path), ".mutation-*")
		if err != nil {
			return errors.New("mutation journal unavailable")
		}
		defer os.Remove(f.Name())
		n, writeErr := f.Write(raw)
		syncErr := f.Sync()
		closeErr := f.Close()
		if n != len(raw) || writeErr != nil || syncErr != nil || closeErr != nil || os.Rename(f.Name(), path) != nil {
			return errors.New("mutation journal update failed")
		}
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("mutation journal directory unavailable")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return errors.New("mutation journal directory sync failed")
	}
	return nil
}

func waitOperation(ctx context.Context, gateway *client.Client, response operations.Response, auth client.Authority) (operations.Response, error) {
	for response.Receipt == nil {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return response, ctx.Err()
		case <-timer.C:
		}
		next, err := gateway.PollOperation(ctx, response.ID, response.RequestSHA256, auth)
		if err != nil {
			return response, err
		}
		response = next
	}
	return response, nil
}

func mutate(ctx context.Context, c Config, key ed25519.PrivateKey, gateway *client.Client, o ExerciseOptions, r operations.Request) (operations.Response, error) {
	digest, err := operations.Digest(r)
	if err != nil {
		return operations.Response{}, err
	}
	j := mutationJournal{Version: 1, Team: o.Team, Subject: o.Subject, Connection: o.Connection, RunID: o.RunID, Request: r, Digest: digest}
	_, statErr := os.Stat(o.Journal)
	recovering := statErr == nil
	if recovering {
		raw, err := readFile(o.Journal, 128<<10, true)
		if err != nil {
			return operations.Response{}, err
		}
		err = operations.DecodeStrict(raw, &j, 128<<10)
		clear(raw)
		actual, digestErr := operations.Digest(j.Request)
		if err != nil || digestErr != nil || j.Version != 1 || j.Team != o.Team || j.Subject != o.Subject || j.Connection != o.Connection || j.RunID != o.RunID || j.Digest != digest || actual != digest || j.Grant == "" {
			return operations.Response{}, errors.New("mutation journal does not match this exact application request")
		}
	} else {
		if !errors.Is(statErr, os.ErrNotExist) {
			return operations.Response{}, errors.New("mutation journal unavailable")
		}
		j.Grant, err = operationGrant(c, key, o, r)
		if err != nil {
			return operations.Response{}, err
		}
		if err := saveJournal(o.Journal, j, true); err != nil {
			return operations.Response{}, err
		}
	}
	auth := client.Authority{OperationGrant: j.Grant}
	var response operations.Response
	switch {
	case j.Response != nil && j.Response.Receipt != nil:
		response = *j.Response
		if response.ValidateBinding(j.ID, digest) != nil {
			return response, errors.New("invalid retained mutation receipt")
		}
	case recovering && j.ID == "":
		response, err = gateway.LookupOperation(ctx, r.IdempotencyKey, digest, auth)
	case recovering:
		response, err = gateway.PollOperation(ctx, j.ID, digest, auth)
	default:
		// This is the only mutation submission. An error never reaches this call again.
		response, err = gateway.SubmitOperation(ctx, r, auth)
	}
	if err != nil {
		return operations.Response{}, errors.New("mutation outcome is unresolved; preserve the journal and reconcile without resubmitting")
	}
	j.ID = response.ID
	j.Response = &response
	if err := saveJournal(o.Journal, j, false); err != nil {
		return response, err
	}
	response, err = waitOperation(ctx, gateway, response, auth)
	if err != nil {
		return response, errors.New("mutation status is unresolved; preserve the journal and reconcile without resubmitting")
	}
	j.Response = &response
	if err := saveJournal(o.Journal, j, false); err != nil {
		return response, err
	}
	if response.Receipt.Outcome != operations.Completed || response.Receipt.Effect != operations.EffectCommitted {
		return response, errors.New("mutation did not confirm a committed effect; do not retry")
	}
	return response, nil
}

// exactSink consumes borrowed batches synchronously and checks a value above
// JavaScript's exact integer range without conversion through floating point.
type exactSink struct {
	id   string
	rows int64
}

func (s *exactSink) Schema(schema *arrow.Schema) error {
	if schema == nil || schema.NumFields() != 2 || schema.Field(0).Name != "id" || schema.Field(1).Name != "value" || (schema.Field(0).Type.ID() != arrow.STRING && schema.Field(0).Type.ID() != arrow.LARGE_STRING) || schema.Field(1).Type.ID() != arrow.INT64 {
		return errors.New("unexpected result schema")
	}
	return nil
}
func (s *exactSink) Write(batch arrow.RecordBatch) error {
	if batch.NumCols() != 2 {
		return errors.New("unexpected result columns")
	}
	value, ok := batch.Column(1).(*array.Int64)
	if !ok {
		return errors.New("integer width changed")
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		if batch.Column(0).IsNull(row) || value.IsNull(row) {
			return errors.New("unexpected null result")
		}
		var id string
		switch ids := batch.Column(0).(type) {
		case *array.String:
			id = ids.Value(row)
		case *array.LargeString:
			id = ids.Value(row)
		default:
			return errors.New("unexpected identifier type")
		}
		if id != s.id || value.Value(row) != ExactValue {
			return errors.New("result value changed")
		}
		s.rows++
	}
	return nil
}

func Exercise(ctx context.Context, c Config, o ExerciseOptions) (report ExerciseReport, err error) {
	report = ExerciseReport{Team: o.Team, Connection: o.Connection}
	stage := "validate_options"
	defer func() {
		if err != nil {
			failure := ExerciseFailure{Stage: stage, Code: exerciseErrorCode(err)}
			report.Failure = &failure
			err = &exerciseStageError{failure: failure, queryState: report.QueryStateOnFailure, queryCode: report.QueryErrorCode, cause: err}
		}
	}()
	if o.Mode == "" {
		o.Mode = "all"
	}
	name := regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
	if !name.MatchString(o.Team) || !name.MatchString(o.Connection) || !name.MatchString(o.RunID) || (o.Mode == "all" && o.Journal == "") || (o.Mode != "all" && o.Mode != "read" && o.Mode != "query") || ((o.ExpectDenied || o.ExpectResolverUnavailable) && o.Mode != "query") || (o.ExpectDenied && o.ExpectResolverUnavailable) {
		return report, ErrConfiguration
	}
	stage = "authorize_workload"
	connection, _, err := (Store{Config: c}).Select(ctx, o.Team, o.Subject, "user", o.Connection, o.Mode == "all")
	if err != nil || (o.Mode == "all" && (connection.WriteSQL == "" || connection.CancelSQL == "")) {
		return report, errors.New("exercise identity or workload is not authorized")
	}
	stage = "load_signing_key"
	key, err := SigningKey(c)
	if err != nil {
		return report, err
	}
	defer clear(key)
	stage = "configure_gateway"
	gateway, err := Gateway(c)
	if err != nil {
		return report, err
	}
	defer gateway.Close()
	runID, _ := json.Marshal(o.RunID)
	ref := operations.ConnectionRef{ID: o.Connection, Database: connection.Database, Schema: connection.Schema}
	write := operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: ref, IdempotencyKey: "example:" + o.Team + ":" + o.Connection + ":" + o.RunID,
		Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: connection.WriteSQL, Transaction: operations.TransactionRequired, Parameters: []operations.Parameter{{Type: "string", Value: runID}, {Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}}}}}
	if o.Mode == "all" {
		stage = "mutation"
		report.Mutation, err = mutate(ctx, c, key, gateway, o, write)
		if err != nil {
			return report, err
		}
		report.Checks = append(report.Checks, "mutation_committed")
	}
	read := operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: ref, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: connection.ReadSQL, Parameters: []operations.Parameter{{Type: "string", Value: runID}}}}}
	if o.Mode != "query" {
		stage = "sign_operation_read"
		token, err := operationGrant(c, key, o, read)
		if err != nil {
			return report, err
		}
		auth := client.Authority{OperationGrant: token}
		stage = "submit_operation_read"
		report.Read, err = gateway.SubmitOperation(ctx, read, auth)
		if err != nil {
			return report, err
		}
		stage = "wait_operation_read"
		report.Read, err = waitOperation(ctx, gateway, report.Read, auth)
		if err != nil {
			return report, err
		}
		stage = "verify_operation_receipt"
		if report.Read.Receipt.Outcome != operations.Completed || report.Read.Receipt.Effect != operations.EffectNone || report.Read.Receipt.Result == nil {
			return report, errors.New("read operation did not produce a verified result")
		}
		readSink := &exactSink{id: o.RunID}
		stage = "decode_operation_result"
		_, err = gateway.DecodeOperationResult(ctx, report.Read, auth, readSink)
		if err != nil {
			return report, err
		}
		report.ReadRows = readSink.rows
		stage = "verify_operation_rows"
		if report.ReadRows != 1 {
			return report, errors.New("read result row count changed")
		}
		report.Checks = append(report.Checks, "operation_result_verified")
	}
	request := query.Request{Mode: "native", ConnectionID: "source_1", SQL: connection.ReadSQL, Parameters: []query.Parameter{{Type: "string", Value: runID}}}
	stage = "sign_analytical_query"
	token, err := queryGrant(c, key, o, connection, request)
	if err != nil {
		return report, err
	}
	queryAuth := client.Authority{Delegation: token}
	stage = "submit_analytical_query"
	handle, err := gateway.Submit(ctx, request, queryAuth)
	if err != nil {
		var failure *client.Error
		if o.ExpectDenied && errors.As(err, &failure) && failure.Code == "PERMISSION_DENIED" {
			report.Checks = append(report.Checks, "query_denied")
			return report, nil
		}
		return report, err
	}
	report.QueryID = handle.ID
	stage = "observe_analytical_status"
	status, err := gateway.Status(ctx, handle.ID, queryAuth)
	if err != nil {
		return report, err
	}
	report.QueryInitialState = status.State
	report.Checks = append(report.Checks, "query_status_observed")
	querySink := &exactSink{id: o.RunID}
	stage = "read_analytical_results"
	report.QueryStats, err = gateway.Results(ctx, handle.ID, queryAuth, querySink)
	if err != nil {
		status, statusErr := queryFailureStatus(ctx, gateway, handle.ID, queryAuth, &report)
		if o.ExpectDenied || o.ExpectResolverUnavailable {
			wantCode, check := "PERMISSION_DENIED", "query_denied"
			if o.ExpectResolverUnavailable {
				wantCode, check = "UNAVAILABLE", "query_failed_resolver_unavailable"
			}
			if statusErr == nil && status.State == "failed" && status.Error != nil && status.Error.Code == wantCode {
				report.Checks = append(report.Checks, check)
				return report, nil
			}
		}
		return report, err
	}
	stage = "verify_analytical_rows"
	if o.ExpectDenied || o.ExpectResolverUnavailable {
		return report, errors.New("query succeeded when a configured failure was required")
	}
	if querySink.rows != 1 || report.QueryStats.Rows != 1 {
		return report, errors.New("analytical result row count changed")
	}
	report.Checks = append(report.Checks, "analytical_result_verified")
	if o.Mode != "all" {
		return report, nil
	}
	request.SQL = connection.CancelSQL
	request.Parameters = nil
	stage = "sign_cancellation_query"
	token, err = queryGrant(c, key, o, connection, request)
	if err != nil {
		return report, err
	}
	queryAuth = client.Authority{Delegation: token}
	stage = "submit_cancellation_query"
	handle, err = gateway.Submit(ctx, request, queryAuth)
	if err != nil {
		return report, err
	}
	report.CancelledQueryID = handle.ID
	stage = "request_cancellation"
	_, err = gateway.Cancel(ctx, handle.ID, queryAuth)
	if err != nil {
		return report, err
	}
	stage = "confirm_cancellation"
	for {
		status, err = gateway.Status(ctx, handle.ID, queryAuth)
		if err != nil {
			return report, err
		}
		report.CancellationState = status.State
		if status.State == "cancelled" {
			break
		}
		if status.State == "failed" || status.State == "succeeded" || status.State == "expired" {
			return report, errors.New("cancellation was not confirmed")
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return report, ctx.Err()
		case <-timer.C:
		}
	}
	report.Checks = append(report.Checks, "cancellation_confirmed")
	return report, nil
}

func WriteReport(writer io.Writer, report ExerciseReport) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
