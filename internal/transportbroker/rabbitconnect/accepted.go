// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

// AcceptedConn belongs to the trusted worker parent. Neither its receipt nor
// its cancellation method may be passed to an untrusted adapter child.
type AcceptedConn struct {
	*tunnelConn
	opener       *Opener
	binding      transportbroker.Binding
	acceptance   transportissuer.AcceptedOpenClaims
	receipt      string
	abortStarted atomic.Bool
}

// Accepted returns detached immutable identifiers for the durable dispatch
// barrier. The caller still owns responsibility for current physical custody.
func (c *AcceptedConn) Accepted() transportissuer.AcceptedOpenClaims { return c.acceptance }

func readAcceptedHeader(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadSlice('\n')
	prefix := []byte(transportissuer.AcceptedOpenHeader + ": ")
	if err != nil || !bytes.HasPrefix(line, prefix) || !bytes.HasSuffix(line, []byte("\r\n")) {
		return "", transportbroker.ErrOpen
	}
	token := string(line[len(prefix) : len(line)-2])
	if len(token) == 0 || len(token) > transportissuer.MaxAcceptedOpenBytes {
		return "", transportbroker.ErrOpen
	}
	end, err := reader.ReadSlice('\n')
	if err != nil || !bytes.Equal(end, []byte("\r\n")) {
		return "", transportbroker.ErrOpen
	}
	return token, nil
}
func (o *Opener) acceptedConnection(conn *tunnelConn, token string, binding transportbroker.Binding) (*AcceptedConn, error) {
	sum := sha256.Sum256([]byte(token))
	digest := hex.EncodeToString(sum[:])
	claims, err := transportissuer.VerifyAcceptedOpen(conn.acceptedReceipt, *o.acceptedTrust, digest, time.Now())
	// The ordinary ticket was already verified. Decode only its original session
	// ceiling; the receipt cannot extend that authenticated ticket or certificate.
	var original ticketClaims
	parts := strings.Split(token, ".")
	raw, decodeErr := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || decodeErr != nil || strictJSON(raw, &original) != nil || claims.ExpiresAt > original.SessionExpiresAt || claims.ExpiresAt > binding.ExpiresAt.Unix() || claims.ExpiresAt > o.certificateUntil.Unix() {
		_, closeErr := reject(conn, transportbroker.ErrScope)
		return nil, closeErr
	}
	return &AcceptedConn{tunnelConn: conn, opener: o, binding: binding, acceptance: claims, receipt: conn.acceptedReceipt}, nil
}
