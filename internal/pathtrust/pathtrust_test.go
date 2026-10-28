// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package pathtrust

import (
	"os"
	"testing"
)

func TestOwnGroupAncestor(t *testing.T) {
	gid := uint32(os.Getegid())
	for _, c := range []struct {
		name    string
		enabled bool
		uid     uint32
		gid     uint32
		mode    uint32
		want    bool
	}{
		{"fsGroup volume root", true, 0, gid, 02775, true},
		{"off", false, 0, gid, 02775, false},
		{"not root owned", true, 1, gid, 02775, false},
		{"other group", true, 0, gid + 1, 02775, false},
		{"world writable", true, 0, gid, 02777, false},
	} {
		if got := ownGroupAncestor(c.enabled, c.uid, c.gid, c.mode); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	if os.Getenv("KELVO_TRUST_OWN_GROUP_ANCESTORS") != "1" && OwnGroupAncestor(0, gid, 02775) {
		t.Fatal("relaxation must be off unless KELVO_TRUST_OWN_GROUP_ANCESTORS=1")
	}
}
