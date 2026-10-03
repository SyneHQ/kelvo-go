// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package readerlease provides bounded, provider-clock reader pins in a
// generation-specific CAS registry. It is groundwork: storage credentials,
// manifest publication and all reader lifetimes must be integrated separately.
// A pin is not authorization and the absence of pins never authorizes deletion.
// There is deliberately no retirement, object listing or garbage collection API.
//
// Callers must hold their existing writer fence during Stage and Seal. Store
// must use a dedicated registry-only identity, remain alive until all leases
// close, honor cancellation, provide current reads and atomic conditional writes,
// and return authenticated provider time with bounded uncertainty. These are
// required provider contracts, not properties inferred from an object URL.
// Local MaxLeases capacity covers in-flight acquisitions and leases across all
// generations, including provider work that outlives a bounded Close. Callers
// must reuse the Registry for its configured scope, not create one per request.
package readerlease
