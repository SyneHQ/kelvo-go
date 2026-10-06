// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

// ValidateStatementReceipt binds ordered statement effects to the exact request.
// Source effects remain valid even when result delivery later fails. Rejected
// operations have no dispatched steps; executed batches describe every entry,
// with trailing unattempted statements explicitly marked none. An unknown
// outcome without steps carries no source progress evidence, as after a lost
// worker; it is never permission to replay the batch.
func ValidateStatementReceipt(request Request, receipt Receipt) error {
	if request.Validate() != nil || receipt.Validate() != nil {
		return ErrInvalid
	}
	if !request.Kind.Mutating() && receipt.Effect != EffectNone || request.Kind.Mutating() && receipt.Outcome == Completed && receipt.Effect != EffectCommitted {
		return ErrInvalid
	}
	digest, err := Digest(request)
	if err != nil || digest != receipt.RequestSHA256 || !validStatementReceipt(request, receipt) {
		return ErrInvalid
	}
	return nil
}

func validStatementReceipt(request Request, receipt Receipt) bool {
	statement := request.Spec.Statement
	if request.Kind != StatementExecute || statement == nil || statement.Batch == nil {
		return len(receipt.Steps) == 0
	}
	if receipt.Outcome == Rejected || receipt.Outcome == CancelledBeforeStart {
		return len(receipt.Steps) == 0
	}
	if receipt.Outcome == OutcomeUnknown && len(receipt.Steps) == 0 {
		return true
	}
	if len(receipt.Steps) != len(statement.Batch.Statements) {
		return false
	}
	var committed, unknown int
	trailingNone := false
	for i, step := range receipt.Steps {
		digest, err := StatementDigest(statement.Batch.Statements[i])
		if err != nil || step.Index != i || step.SHA256 != digest {
			return false
		}
		switch receipt.Outcome {
		case Completed:
			if step.Effect != EffectCommitted {
				return false
			}
		case Failed:
			if step.Effect == EffectNone {
				trailingNone = true
				continue
			}
			if step.Effect != EffectCommitted || statement.Transaction == TransactionRequired || trailingNone {
				return false
			}
			committed++
		case OutcomeUnknown:
			if step.Effect == EffectNone {
				trailingNone = true
				continue
			}
			if trailingNone {
				return false
			}
			if step.Effect == EffectUnknown {
				unknown++
				if statement.Transaction == TransactionAutocommit && unknown > 1 {
					return false
				}
				continue
			}
			if step.Effect != EffectCommitted || statement.Transaction == TransactionRequired || unknown != 0 {
				return false
			}
		default:
			return false
		}
	}
	if receipt.Outcome == Failed {
		return (committed == 0 && receipt.Effect == EffectNone) || (committed > 0 && receipt.Effect == EffectPartial)
	}
	return receipt.Outcome != OutcomeUnknown || unknown > 0
}
