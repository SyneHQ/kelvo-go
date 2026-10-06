// Package server provides the private adapter HTTP boundary. It deliberately
// has no default executor: operation grants, dispatch custody and the durable
// receipt store must be supplied before this server can be constructed.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// Executor must verify the grant's tenant, operation digest, adapter identity,
// expiry and assigned custody before resolving any credentials or executing.
// It records write dispatch durably and never retries an uncertain mutation.
type Executor interface {
	Execute(context.Context, string, string, operations.Request) (operations.Receipt, error)
}

type Config struct {
	TLS           *tls.Config
	WorkerURIs    []string
	Executor      Executor
	MaxConcurrent int
	Timeout       time.Duration
}

// New builds a server without listening. Run it with ServeTLS; the handler also
// checks verified TLS state so accidentally calling Serve fails closed.
func New(config Config) (*http.Server, error) {
	if config.Executor == nil || config.TLS == nil || len(config.TLS.Certificates) == 0 ||
		config.TLS.ClientCAs == nil || config.MaxConcurrent < 1 || config.MaxConcurrent > 256 ||
		config.Timeout < time.Second || config.Timeout > 10*time.Minute || len(config.WorkerURIs) == 0 {
		return nil, errors.New("invalid private adapter server configuration")
	}
	peers := make(map[string]bool, len(config.WorkerURIs))
	for _, peer := range config.WorkerURIs {
		if !strings.HasPrefix(peer, "spiffe://") || len(peer) > 512 {
			return nil, errors.New("invalid adapter worker identity")
		}
		peers[peer] = true
	}
	tlsConfig := config.TLS.Clone()
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	tlsConfig.SessionTicketsDisabled = true
	admission := make(chan struct{}, config.MaxConcurrent)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if !authorizedPeer(r, peers) {
			publicError(w, http.StatusUnauthorized, "worker_identity_required")
			return
		}
		if r.URL.Path != "/v1/operations" || r.Method != http.MethodPost {
			publicError(w, http.StatusNotFound, "not_found")
			return
		}
		id := r.Header.Get("Kelvo-Operation-ID")
		grant, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !operations.ValidID(id) || !ok || len(grant) == 0 || len(grant) > 8192 || strings.ContainsAny(grant, " \t\r\n") {
			publicError(w, http.StatusUnauthorized, "operation_authority_required")
			return
		}
		select {
		case admission <- struct{}{}:
			defer func() { <-admission }()
		default:
			w.Header().Set("Retry-After", "1")
			publicError(w, http.StatusTooManyRequests, "adapter_busy")
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, operations.MaxRequestBytes))
		if err != nil {
			publicError(w, http.StatusBadRequest, "invalid_operation")
			return
		}
		request, err := operations.ParseRequest(raw)
		if err != nil {
			publicError(w, http.StatusBadRequest, "invalid_operation")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), config.Timeout)
		defer cancel()
		digest, err := operations.Digest(request)
		if err != nil {
			publicError(w, http.StatusBadRequest, "invalid_operation")
			return
		}
		// Synchronous execution keeps admission until the actual backend exits,
		// even if a legacy driver ignores context cancellation during its dial.
		receipt, err := config.Executor.Execute(ctx, id, grant, request)
		if err != nil {
			publicError(w, http.StatusBadGateway, "operation_outcome_unavailable")
			return
		}
		if receipt.Validate() != nil || receipt.OperationID != id || receipt.RequestSHA256 != digest {
			publicError(w, http.StatusBadGateway, "invalid_operation_receipt")
			return
		}
		encoded, err := json.Marshal(receipt)
		if err != nil || len(encoded) > operations.MaxReceiptBytes {
			publicError(w, http.StatusBadGateway, "invalid_operation_receipt")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encoded)
	})
	return &http.Server{
		Handler: handler, TLSConfig: tlsConfig,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: config.Timeout + 5*time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}, nil
}

func authorizedPeer(r *http.Request, peers map[string]bool) bool {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return false
	}
	now := time.Now()
	chainValid := false
	for _, chain := range r.TLS.VerifiedChains {
		valid := len(chain) > 0
		for _, certificate := range chain {
			valid = valid && !now.Before(certificate.NotBefore) && now.Before(certificate.NotAfter)
		}
		chainValid = chainValid || valid
	}
	if !chainValid {
		return false
	}
	for _, uri := range r.TLS.PeerCertificates[0].URIs {
		if peers[uri.String()] {
			return true
		}
	}
	return false
}

func publicError(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: code})
}
