// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

const MaxSealedInputBytes = 1 << 20

// InputReference returns a copy of the sole sealed input, if present. The
// caller must validate Request and authorize the returned reference separately.
func (r Request) InputReference() *InputRef {
	var ref *InputRef
	switch {
	case r.Spec.Schema != nil:
		ref = &r.Spec.Schema.Plan
	case r.Spec.Migration != nil && r.Kind == MigrationApply:
		ref = &r.Spec.Migration.Plan
	case r.Spec.Native != nil:
		ref = r.Spec.Native.Input
	case r.Spec.Ingestion != nil:
		ref = r.Spec.Ingestion.Input
	case r.Spec.Watch != nil:
		ref = r.Spec.Watch.Checkpoint
		if r.Kind == WatchInstall {
			ref = r.Spec.Watch.Resume
		}
	}
	if ref == nil {
		return nil
	}
	copy := *ref
	return &copy
}
