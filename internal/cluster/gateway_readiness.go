// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	gatewayReadinessInterval = 5 * time.Second
	gatewayReadinessTimeout  = 2 * time.Second
	gatewayReadinessParallel = 8
	gatewayReadinessBodyMax  = 4096
)

// Each entry is a health snapshot, never a reservation of worker capacity.
// Expiry also bounds stale success if a sweep is delayed by other workers.
type gatewayWorkerHealth map[string]map[string]time.Time

// workersReadyLocked reads only cached evidence; public probes do no network I/O.
func (g *Gateway) workersReadyLocked(now time.Time) bool {
	if len(g.tenants) == 0 || (!g.workerCertificateUntil.IsZero() && !now.Before(g.workerCertificateUntil)) {
		return false
	}
	for tenant, configured := range g.tenants {
		ready := false
		for worker := range configured.workers {
			if now.Before(g.workerHealth[tenant][worker]) {
				ready = true
				break
			}
		}
		if !ready {
			return false
		}
	}
	return true
}

// One loop coalesces refreshes. A slow sweep never starts another sweep beside it.
func (g *Gateway) watchWorkerReadiness() {
	defer g.wg.Done()
	tick := time.NewTicker(gatewayReadinessInterval)
	defer tick.Stop()
	for {
		g.refreshWorkerReadiness()
		select {
		case <-g.ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (g *Gateway) refreshWorkerReadiness() {
	type probe struct {
		tenant, worker string
		endpoint       workerEndpoint
	}
	g.mu.RLock()
	if g.draining || g.closed || g.ctx.Err() != nil {
		g.mu.RUnlock()
		return
	}
	var probes []probe
	for tenant, configured := range g.tenants {
		for worker, endpoint := range configured.workers {
			probes = append(probes, probe{tenant, worker, endpoint})
		}
	}
	g.mu.RUnlock()
	jobs := make(chan probe)
	var workers sync.WaitGroup
	for range min(gatewayReadinessParallel, len(probes)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				g.mu.RLock()
				stopped := g.draining || g.closed || g.ctx.Err() != nil
				g.mu.RUnlock()
				if stopped {
					continue
				}
				until := g.probeWorkerReadiness(job.endpoint)
				g.mu.Lock()
				if g.workerHealth == nil {
					g.workerHealth = gatewayWorkerHealth{}
				}
				if g.workerHealth[job.tenant] == nil {
					g.workerHealth[job.tenant] = map[string]time.Time{}
				}
				g.workerHealth[job.tenant][job.worker] = until
				g.mu.Unlock()
			}
		}()
	}
send:
	for _, job := range probes {
		select {
		case <-g.ctx.Done():
			break send
		case jobs <- job:
		}
	}
	close(jobs)
	workers.Wait()
}

func (g *Gateway) probeWorkerReadiness(endpoint workerEndpoint) time.Time {
	if endpoint.url == nil || endpoint.client == nil {
		return time.Time{}
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(g.ctx, gatewayReadinessTimeout)
	defer cancel()
	u := *endpoint.url
	u.Path = strings.TrimRight(u.Path, "/") + "/ready"
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return time.Time{}
	}
	req.Header.Set("Cache-Control", "no-cache")
	response, err := endpoint.client.Do(req)
	if err != nil {
		return time.Time{}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		return time.Time{}
	}
	read, err := io.Copy(io.Discard, io.LimitReader(response.Body, gatewayReadinessBodyMax+1))
	if err != nil || read > gatewayReadinessBodyMax || ctx.Err() != nil {
		return time.Time{}
	}
	// A reused TLS connection must not extend health past certificate validity.
	// Convert wall-clock certificate dates without discarding monotonic expiry.
	now := time.Now()
	until := started.Add(gatewayReadinessInterval + gatewayReadinessTimeout)
	for _, chain := range response.TLS.VerifiedChains {
		if len(chain) == 0 {
			return time.Time{}
		}
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				return time.Time{}
			}
			until = minTime(until, now.Add(cert.NotAfter.Sub(now)))
		}
	}
	if !g.workerCertificateUntil.IsZero() {
		until = minTime(until, now.Add(g.workerCertificateUntil.Sub(now)))
	}
	if !now.Before(until) {
		return time.Time{}
	}
	return until
}

// Static client certificates share one lifecycle across all endpoint clients.
// Rotating identities are checked by the existing gateway/transport gates.
func gatewayStaticCertificateExpiry(config *tls.Config) time.Time {
	var until time.Time
	for _, cert := range config.Certificates {
		for _, raw := range cert.Certificate {
			parsed, err := x509.ParseCertificate(raw)
			if err != nil {
				return time.Unix(0, 0)
			}
			if until.IsZero() || parsed.NotAfter.Before(until) {
				until = parsed.NotAfter
			}
		}
	}
	return until
}
