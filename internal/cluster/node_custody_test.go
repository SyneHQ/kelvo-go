// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type custodyQueryExecutor struct{ pool *admission.Pool }

func (e custodyQueryExecutor) Execute(ctx context.Context, request query.Request, sink query.Sink) (query.Stats, error) {
	if request.SQL == "SELECT 1" {
		return query.Stats{}, nil
	}
	reservation, err := e.pool.Acquire(ctx, admission.Request{MemoryBytes: 1})
	if err != nil {
		return query.Stats{}, err
	}
	custody, _ := containment.NewCustody(reservation.Release)
	defer custody.Complete()
	if err := containment.RetainUntilCompletion(ctx, custody); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{}, sink.Schema(arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil))
}

type custodyQueryStore struct {
	*nodeTestStore
	t         *testing.T
	pool      *admission.Pool
	fail      bool
	published bool
}

func (s *custodyQueryStore) CompareAndSwap(ctx context.Context, old Snapshot, next Job) (Snapshot, error) {
	if next.State == ResultReady {
		if s.pool.Snapshot().Active != 1 {
			s.t.Error("receipt publication lost its resource reservation")
		}
		s.published = true
		if s.fail {
			return Snapshot{}, errors.New("fixture publication failure")
		}
	}
	return s.nodeTestStore.CompareAndSwap(ctx, old, next)
}

type custodyResponseWriter struct {
	*httptest.ResponseRecorder
	t       *testing.T
	pool    *admission.Pool
	failEOS bool
}

func (w custodyResponseWriter) Write(p []byte) (int, error) {
	if w.pool.Snapshot().Active != 1 {
		w.t.Error("Arrow delivery lost its resource reservation")
	}
	if w.failEOS && len(p) == 8 && string(p) == "\xff\xff\xff\xff\x00\x00\x00\x00" {
		return 0, errors.New("fixture EOS delivery failure")
	}
	return w.ResponseRecorder.Write(p)
}

func TestNodeCustodySpansResultPublicationAndEOS(t *testing.T) {
	for _, mode := range []string{"success", "publication failure", "EOS failure"} {
		t.Run(mode, func(t *testing.T) {
			pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			store := &custodyQueryStore{nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}, t: t, pool: pool, fail: mode == "publication failure"}
			node, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1"}, store, custodyQueryExecutor{pool})
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			store.add(id)
			waitFor(t, func() bool { s, _ := store.Get(context.Background(), id); return s.Job.State == Assigned })
			old, _ := store.Get(context.Background(), id)
			next := old.Job
			next.State, next.Claim = Claimed, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			if _, err := store.CompareAndSwap(context.Background(), old, next); err != nil {
				t.Fatal(err)
			}
			uri, _ := url.Parse(GatewayIdentity)
			cert := &x509.Certificate{URIs: []*url.URL{uri}}
			request := httptest.NewRequest(http.MethodGet, "/internal/queries/"+id+"/results", nil)
			request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
			request.Header.Set("X-Kelvo-Claim", next.Claim)
			writer := custodyResponseWriter{ResponseRecorder: httptest.NewRecorder(), t: t, pool: pool, failEOS: mode == "EOS failure"}
			func() {
				defer func() {
					if v := recover(); v != nil && v != http.ErrAbortHandler {
						panic(v)
					}
				}()
				node.ServeHTTP(writer, request)
			}()
			if !store.published {
				t.Fatal("fixture never reached receipt publication")
			}
			if pool.Snapshot().Active != 0 {
				t.Fatal("completed handler retained its reservation")
			}
		})
	}
}
