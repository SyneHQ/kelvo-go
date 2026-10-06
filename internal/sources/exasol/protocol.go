// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exasol

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/exasol/exasol-driver-go/pkg/types"
	"github.com/gorilla/websocket"
)

type session struct {
	conn                                *websocket.Conn
	writeMu                             sync.Mutex
	responseLimit, wireLimit, wireBytes int64
	usable                              bool
}

func (s *session) write(deadline time.Time, message any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return s.conn.WriteJSON(message)
}
func (s *session) exchange(ctx context.Context, request, output any, cleanup bool) error {
	if !s.usable {
		return query.NewError("QUERY_FAILED", "Exasol session is unavailable")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	limit := s.responseLimit
	if cleanup {
		limit = min(limit, 128<<10)
	} else {
		limit = min(limit, s.wireLimit-s.wireBytes)
	}
	if limit <= 0 {
		return query.NewError("RESOURCE_EXHAUSTED", "Exasol response budget exhausted")
	}
	deadline, _ := ctx.Deadline()
	if err := s.write(deadline, request); err != nil {
		s.usable = false
		return public(ctx, "Could not send Exasol request")
	}
	readDeadline := deadline
	if !cleanup {
		readDeadline = deadline.Add(2 * time.Second)
	}
	s.conn.SetReadDeadline(readDeadline)
	s.conn.SetReadLimit(limit)
	aborted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(aborted)
		if cleanup {
			_ = s.conn.UnderlyingConn().SetReadDeadline(time.Now())
			return
		}
		// abortQuery has no response. Drain the outstanding command's reply before
		// reusing the session for rollback. Unresponsive sessions are discarded.
		until := time.Now().Add(2 * time.Second)
		_ = s.conn.UnderlyingConn().SetReadDeadline(until)
		_ = s.write(until, map[string]any{"command": "abortQuery"})
	})
	defer func() {
		if !stop() {
			<-aborted
		}
	}()
	messageType, body, err := s.conn.ReadMessage()
	s.wireBytes += int64(len(body))
	if err != nil {
		s.usable = false
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, websocket.ErrReadLimit) {
			return query.NewError("RESOURCE_EXHAUSTED", "Exasol response exceeds memory budget")
		}
		return public(ctx, "Could not read Exasol response")
	}
	if messageType != websocket.TextMessage {
		s.usable = false
		return query.NewError("QUERY_FAILED", "Exasol returned an unsupported message encoding")
	}
	var envelope struct {
		Status     string          `json:"status"`
		Data       json.RawMessage `json:"responseData"`
		Attributes json.RawMessage `json:"attributes"`
		Exception  json.RawMessage `json:"exception"`
	}
	if err := decode(body, &envelope); err != nil {
		s.usable = false
		return query.NewError("QUERY_FAILED", "Exasol returned invalid JSON")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if envelope.Status != "ok" || len(envelope.Exception) != 0 && string(envelope.Exception) != "null" {
		return query.NewError("QUERY_FAILED", "Exasol rejected the request")
	}
	if output != nil {
		if _, attributes := output.(*sessionAttributes); attributes {
			envelope.Data = envelope.Attributes
		}
		if len(envelope.Data) == 0 || string(envelope.Data) == "null" || decode(envelope.Data, output) != nil {
			return query.NewError("QUERY_FAILED", "Exasol returned invalid response data")
		}
	}
	return nil
}
func decode(body []byte, output any) error {
	if !utf8.Valid(body) {
		return errors.New("invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func (s *session) login(ctx context.Context, config *dsn.DSNConfig, limits query.Limits) error {
	var key types.PublicKeyResponse
	if err := s.exchange(ctx, map[string]any{"command": "login", "protocolVersion": 2}, &key, false); err != nil {
		return err
	}
	if len(key.PublicKeyModulus) < 256 || len(key.PublicKeyModulus) > 2048 || len(key.PublicKeyExponent) > 8 {
		return query.NewError("QUERY_FAILED", "Exasol returned an unsupported login key")
	}
	modulus, err := hex.DecodeString(key.PublicKeyModulus)
	exponent, expErr := strconv.ParseUint(key.PublicKeyExponent, 16, 31)
	if err != nil || expErr != nil || exponent < 3 || exponent%2 != 1 {
		return query.NewError("QUERY_FAILED", "Exasol returned an invalid login key")
	}
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: int(exponent)}
	if pub.N.BitLen() < 1024 || pub.N.BitLen() > 8192 {
		return query.NewError("QUERY_FAILED", "Exasol returned an unsupported login key")
	}
	encrypted, err := rsa.EncryptPKCS1v15(rand.Reader, pub, []byte(config.Password))
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Exasol password cannot be encoded for this server")
	}
	timeout := max(1, int((limits.Timeout+time.Second-1)/time.Second))
	if config.QueryTimeout > 0 {
		timeout = min(timeout, config.QueryTimeout)
	}
	attributes := map[string]any{"autocommit": false, "queryTimeout": timeout, "timestampUtcEnabled": true}
	if config.Schema != "" {
		attributes["currentSchema"] = config.Schema
	}
	var auth types.AuthResponse
	if err := s.exchange(ctx, map[string]any{"username": config.User, "password": base64.StdEncoding.EncodeToString(encrypted), "useCompression": false, "clientName": "Kelvo", "driverName": "kelvo-exasol-websocket", "attributes": attributes}, &auth, false); err != nil {
		return err
	}
	if auth.ProtocolVersion != 2 {
		return query.NewError("UNSUPPORTED", "Exasol did not negotiate protocol version 2")
	}
	var applied sessionAttributes
	if err := s.exchange(ctx, map[string]any{"command": "getAttributes"}, &applied, false); err != nil {
		return err
	}
	if applied.Autocommit == nil || *applied.Autocommit || applied.TimestampUTC == nil || !*applied.TimestampUTC {
		return query.NewError("QUERY_FAILED", "Exasol did not enforce required session attributes")
	}
	return nil
}
func (s *session) cleanup(handle *int64) error {
	if !s.usable {
		return query.NewError("QUERY_FAILED", "Exasol session closed before rollback acknowledgement")
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	var first error
	if handle != nil {
		first = s.exchange(ctx, map[string]any{"command": "closeResultSet", "resultSetHandles": []int64{*handle}}, nil, true)
	}
	if err := s.exchange(ctx, map[string]any{"command": "execute", "sqlText": "ROLLBACK", "attributes": map[string]any{"autocommit": false}}, nil, true); first == nil {
		first = err
	}
	if err := s.exchange(ctx, map[string]any{"command": "disconnect"}, nil, true); first == nil {
		first = err
	}
	return first
}

type results struct {
	Count   int      `json:"numResults"`
	Results []result `json:"results"`
}
type sessionAttributes struct {
	Autocommit   *bool `json:"autocommit"`
	TimestampUTC *bool `json:"timestampUtcEnabled"`
}
type result struct {
	Kind     string     `json:"resultType"`
	Set      *resultSet `json:"resultSet"`
	RowCount *int64     `json:"rowCount"`
}
type resultSet struct {
	Handle        *int64                 `json:"resultSetHandle"`
	NumColumns    int                    `json:"numColumns"`
	NumRows       *int64                 `json:"numRows"`
	RowsInMessage *int64                 `json:"numRowsInMessage"`
	Columns       []types.SqlQueryColumn `json:"columns"`
	Data          [][]any                `json:"data"`
}
