// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package resolver

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

// CleanupBinding selects retained physical cleanup custody. It does not renew
// normal execution authority or permit another source operation.
type CleanupBinding struct {
	WorkerID         string `json:"worker_id"`
	Owner            string `json:"owner"`
	Claim            string `json:"claim"`
	DataTicketSHA256 string `json:"data_ticket_sha256"`
	AcceptanceID     string `json:"acceptance_id"`
}

func (b CleanupBinding) Validate() error {
	if (Binding{WorkerID: b.WorkerID, Owner: b.Owner, Claim: b.Claim}).Validate() != nil || !operations.ValidDigest(b.DataTicketSHA256) || len(b.AcceptanceID) != 32 {
		return ErrInvalid
	}
	for _, c := range b.AcceptanceID {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ErrInvalid
		}
	}
	return nil
}

type CleanupLeaseResponse = transportissuer.CleanupLeaseResponse

// CleanupLeaseVerifier reads the original cancellation cutoff. It must not
// create cleanup custody, renew execution authority or extend that cutoff.
type CleanupLeaseVerifier interface {
	ValidateOperationCleanupLease(context.Context, string, string, CleanupBinding) (CleanupLeaseResponse, error)
}
