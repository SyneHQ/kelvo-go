// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

const MaxPostgresCancelKeyBytes = 256

// PostgresTarget is constructed by the trusted parent from its original source
// TLS policy and a single authenticated-adapter registration. An adapter cannot
// supply replacement TLS settings, a destination or new backend target at abort.
type PostgresTarget struct {
	PID    uint32
	Secret []byte
	TLS    *tls.Config
}

// AbortPostgres sends one typed cancellation packet through the accepted
// source's cleanup-only tunnel. Success means delivery, NOT query termination.
// The original DATA session must independently observe protocol completion.
func (c *AcceptedConn) AbortPostgres(parent context.Context, target PostgresTarget) (result error) {
	if c == nil || parent == nil || c.closed.Load() || target.PID == 0 || len(target.Secret) < 4 || len(target.Secret) > MaxPostgresCancelKeyBytes || target.TLS == nil || target.TLS.InsecureSkipVerify || target.TLS.ServerName == "" || target.TLS.MinVersion < tls.VersionTLS12 {
		return transportbroker.ErrInvalid
	}
	issuer, ok := c.opener.tickets.(CleanupIssuer)
	if !ok || !c.abortStarted.CompareAndSwap(false, true) {
		return transportbroker.ErrScope
	}
	target.Secret = bytes.Clone(target.Secret)
	defer clear(target.Secret)
	target.TLS = target.TLS.Clone()
	ctx, cancel := context.WithDeadline(parent, minTime(time.Now().Add(transportissuer.MaxCleanupLifetime), time.Unix(c.acceptance.ExpiresAt, 0)))
	defer cancel()
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return transportbroker.ErrOpen
	}
	request := transportissuer.CleanupRequest{Version: 1, DataTicketSHA256: c.acceptance.DataTicketSHA256, AcceptedOpen: c.receipt, OpenID: hex.EncodeToString(nonce[:]), Protocol: transportissuer.PostgresCancel}
	token, err := issuer.IssueCleanup(ctx, request)
	if err != nil || c.closed.Load() {
		return transportbroker.ErrOpen
	}
	o := c.opener
	binding := transportissuer.AbortBinding{DataTicketSHA256: c.acceptance.DataTicketSHA256, AcceptanceID: c.acceptance.AcceptanceID, WorkerIdentity: o.identity, WorkerCertSHA256: o.certDigest}
	claims, err := transportissuer.VerifyPostgresAbort(token, transportissuer.AbortTrust{Issuer: o.issuer, Audience: o.audience, PublicKey: o.key}, binding, time.Now())
	if err != nil || claims.ID != request.OpenID || claims.ExpiresAt > c.acceptance.ExpiresAt || claims.ExpiresAt > o.certificateUntil.Unix() {
		return transportbroker.ErrScope
	}
	bounded, stop := context.WithDeadline(ctx, time.Unix(claims.ExpiresAt, 0))
	defer stop()
	conn, err := o.connect(bounded, c.binding.Authority, token, false, true)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, conn.Close()) }()
	if c.closed.Load() {
		return transportbroker.ErrClosed
	}
	until, _ := bounded.Deadline()
	if conn.SetDeadline(until) != nil {
		return transportbroker.ErrOpen
	}
	closeDone := make(chan struct{})
	closeStop := context.AfterFunc(bounded, func() { defer close(closeDone); _ = conn.Close() })
	defer func() {
		if !closeStop() {
			<-closeDone
		}
	}()
	// Current native source configuration permits verify-full PostgreSQL TLS
	// with SSLRequest negotiation. It does not permit plaintext or direct-TLS.
	ssl := []byte{0, 0, 0, 8, 4, 210, 22, 47}
	if n, err := conn.Write(ssl); err != nil || n != len(ssl) {
		return transportbroker.ErrOpen
	}
	var response [1]byte
	if _, err := io.ReadFull(conn, response[:]); err != nil || response[0] != 'S' {
		return transportbroker.ErrOpen
	}
	secure := tls.Client(conn, target.TLS)
	if err := secure.HandshakeContext(bounded); err != nil {
		return transportbroker.ErrOpen
	}
	state := secure.ConnectionState()
	if !state.HandshakeComplete || len(state.VerifiedChains) == 0 {
		return transportbroker.ErrOpen
	}
	packet := make([]byte, 12+len(target.Secret))
	defer clear(packet)
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], 80877102)
	binary.BigEndian.PutUint32(packet[8:12], target.PID)
	copy(packet[12:], target.Secret)
	if n, err := secure.Write(packet); err != nil || n != len(packet) {
		return transportbroker.ErrOpen
	}
	// A peer close only completes delivery. No SQL is sent and no arbitrary
	// protocol response is accepted on this parent-only connection.
	var end [1]byte
	n, err := secure.Read(end[:])
	if n != 0 || (err != io.EOF && !errors.Is(err, net.ErrClosed)) || bounded.Err() != nil {
		return transportbroker.ErrOpen
	}
	return nil
}
