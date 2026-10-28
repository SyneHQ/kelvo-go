// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package pathtrust holds the opt-in relaxation for ancestor directories that
// Kubernetes creates for a pod fsGroup volume (root:<fsGroup>, mode 2775).
package pathtrust

import "os"

var ownGroupAncestors = os.Getenv("KELVO_TRUST_OWN_GROUP_ANCESTORS") == "1"

// OwnGroupAncestor reports whether a group-writable ANCESTOR directory is
// acceptable: opt-in only, root-owned, group is this process's effective group,
// and not world-writable. Callers must never apply it to Kelvo's own directories
// or files.
func OwnGroupAncestor(uid, gid, mode uint32) bool {
	return ownGroupAncestor(ownGroupAncestors, uid, gid, mode)
}

func ownGroupAncestor(enabled bool, uid, gid, mode uint32) bool {
	return enabled && uid == 0 && gid == uint32(os.Getegid()) && mode&0o002 == 0
}
