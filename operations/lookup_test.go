// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"strings"
	"testing"
)

func TestLookupRequestRequiresExactRetainedIdentity(t *testing.T) {
	valid := LookupRequest{Version: Version, IdempotencyKey: "job-attempt-1", RequestSHA256: strings.Repeat("a", 64)}
	if valid.Validate() != nil {
		t.Fatal("valid lookup rejected")
	}
	for _, change := range []func(*LookupRequest){
		func(r *LookupRequest) { r.Version++ },
		func(r *LookupRequest) { r.IdempotencyKey = "" },
		func(r *LookupRequest) { r.IdempotencyKey = strings.Repeat("a", 129) },
		func(r *LookupRequest) { r.IdempotencyKey = "job\n1" },
		func(r *LookupRequest) { r.RequestSHA256 = strings.Repeat("A", 64) },
		func(r *LookupRequest) { r.RequestSHA256 = "" },
	} {
		request := valid
		change(&request)
		if request.Validate() == nil {
			t.Fatal("invalid lookup accepted")
		}
	}
}
