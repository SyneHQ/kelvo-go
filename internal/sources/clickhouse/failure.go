// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"os"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// Failure records why a ClickHouse request failed in a fixed vocabulary. It
// wraps the unchanged public error; a query child or adapter reports the
// embedded SourceFailure to the node, which logs it.
type Failure struct {
	operations.SourceFailure
	err error
}

func (f *Failure) Error() string { return f.err.Error() }
func (f *Failure) Unwrap() error { return f.err }

func failed(ctx context.Context, public error, reason, host string, response *http.Response, cause error) error {
	f := &Failure{SourceFailure: operations.SourceFailure{Reason: reason, Host: host}, err: public}
	if ctx.Err() != nil {
		f.Reason = "canceled"
	} else if cause != nil {
		f.Reason = classify(cause)
	}
	if response != nil {
		f.HTTPStatus = response.StatusCode
		f.ClickHouseCode = operations.ExceptionCode(response.Header.Get("X-ClickHouse-Exception-Code"))
	}
	return f
}

func classify(err error) string {
	var dns *net.DNSError
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var verify *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	var alert tls.AlertError
	var invalid x509.CertificateInvalidError
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.As(err, &dns):
		return "dns"
	case errors.As(err, &unknown):
		return "tls_unknown_authority"
	case errors.As(err, &hostname):
		return "tls_hostname"
	case errors.As(err, &verify), errors.As(err, &record), errors.As(err, &alert), errors.As(err, &invalid):
		return "tls_other"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connect_refused"
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return "connect_timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "connect_timeout"
	}
	return "other"
}
