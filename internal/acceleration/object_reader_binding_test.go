// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

func objectReaderBindingFixture() (readerlease.Reference, catalog.ObjectLocation, objectCommitted) {
	ref := readerlease.Reference{Tenant: "analytics", Dataset: "events", Generation: "00112233445566778899aabbccddeeff",
		Incarnation: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	location := catalog.ObjectLocation{Provider: "s3", Endpoint: "https://objects.example.test", Bucket: "snapshots", Prefix: "kelvo/production", Region: "us-east-1"}
	commit := objectCommitted{Generation: ref.Generation, Fingerprint: "policy-v1", SHA256: strings.Repeat("a", 64), SchemaHash: strings.Repeat("b", 64),
		Rows: 9876543210, Bytes: 98304, RefreshedAt: time.Date(2026, 10, 4, 12, 34, 56, 123456789, time.UTC), ObjectVersion: "object-v1"}
	return ref, location, commit
}

func objectReaderBindingMultipart(commit *objectCommitted) {
	commit.ObjectVersion = ""
	commit.Descriptor = &objectDescriptorRef{ObjectVersion: "descriptor-v1", Bytes: 512, PartCount: 2}
}

func TestObjectReaderBindingCanonicalVectors(t *testing.T) {
	// These byte vectors were authored independently of the Go encoder. Keep
	// the framing, field order, domain and version pinned across implementations.
	const header = "0000001b6b656c766f2f6f626a6563742d7265616465722d62696e64696e67" + // domain
		"00000001" + // format version
		"000000027333" + // provider
		"0000001c68747470733a2f2f6f626a656374732e6578616d706c652e74657374" + // endpoint
		"00000009736e617073686f7473" + // bucket
		"000000106b656c766f2f70726f64756374696f6e" + // prefix
		"0000000975732d656173742d31" + // region
		"00000000" + // account
		"00000009616e616c7974696373" + // tenant
		"000000066576656e7473" + // dataset
		"000000203030313132323333343435353636373738383939616162626363646465656666" + // generation
		"00000040" + // incarnation
		"30313233343536373839616263646566" + "30313233343536373839616263646566" +
		"30313233343536373839616263646566" + "30313233343536373839616263646566"
	const checksum = "0000004061616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161"
	const tail = "0000004062626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262" + // schema
		"00000009706f6c6963792d7631" + // fingerprint
		"000000006ac247f0075bcd15" // UTC seconds and nanoseconds
	for _, tc := range []struct {
		name, vector, digest string
		multipart            bool
	}{
		{"single", header + "01" + checksum + "000000096f626a6563742d7631" +
			"000000000000000000000000000000024cb016ea0000000000018000" + tail,
			"542af91d27e34efef3e595b218e75c7a843dde74eb21c5343ebec73c0f388038", false},
		{"multipart", header + "02" + checksum + "0000000d64657363726970746f722d7631" +
			"000000000000020000000002000000024cb016ea0000000000018000" + tail,
			"33fce0fe0bb2b87769f5be6f095e419b09fefd45700caa9cdd41185a88e78bea", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vector, err := hex.DecodeString(tc.vector)
			if err != nil {
				t.Fatal(err)
			}
			vectorDigest := sha256.Sum256(vector)
			if hex.EncodeToString(vectorDigest[:]) != tc.digest {
				t.Fatal("pinned canonical vector is inconsistent")
			}
			ref, location, committed := objectReaderBindingFixture()
			if tc.multipart {
				objectReaderBindingMultipart(&committed)
			}
			binding, err := objectReaderBinding(ref, location, &committed)
			if err != nil || binding.Reference != ref || binding.ContentSHA256 != tc.digest {
				t.Fatalf("binding = %+v, err = %v; want reference and digest %s", binding, err, tc.digest)
			}
		})
	}
}

func TestObjectReaderBindingEveryIdentityField(t *testing.T) {
	type fixtureChange func(*readerlease.Reference, *catalog.ObjectLocation, *objectCommitted)
	multipart := func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
		objectReaderBindingMultipart(c)
	}
	r2 := func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) {
		l.Provider, l.Region = "r2", "auto"
	}
	azure := func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) {
		l.Provider, l.Region, l.Account = "azure", "", "accountone"
	}
	for _, tc := range []struct {
		name            string
		prepare, change fixtureChange
	}{
		{"provider", r2, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) { l.Provider = "gcs" }},
		{"endpoint", nil, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) {
			l.Endpoint = "https://other.example.test"
		}},
		{"endpoint-spelling", nil, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) {
			l.Endpoint = "https://OBJECTS.example.test"
		}},
		{"bucket", nil, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) {
			l.Bucket = "other-snapshots"
		}},
		{"prefix", nil, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) { l.Prefix += "/other" }},
		{"region", nil, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) { l.Region = "us-west-2" }},
		{"automatic-region-spelling", r2, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) { l.Region = "" }},
		{"account", azure, func(_ *readerlease.Reference, l *catalog.ObjectLocation, _ *objectCommitted) {
			l.Account = "accounttwo"
		}},
		{"tenant", nil, func(r *readerlease.Reference, _ *catalog.ObjectLocation, _ *objectCommitted) {
			r.Tenant = "other-tenant"
		}},
		{"dataset", nil, func(r *readerlease.Reference, _ *catalog.ObjectLocation, _ *objectCommitted) {
			r.Dataset = "other_events"
		}},
		{"generation", nil, func(r *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			r.Generation, c.Generation = strings.Repeat("c", 32), strings.Repeat("c", 32)
		}},
		{"incarnation", nil, func(r *readerlease.Reference, _ *catalog.ObjectLocation, _ *objectCommitted) {
			r.Incarnation = strings.Repeat("c", 64)
		}},
		{"kind", nil, multipart},
		{"checksum", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.SHA256 = strings.Repeat("c", 64)
		}},
		{"object-version", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.ObjectVersion += "-other"
		}},
		{"opaque-version-space", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.ObjectVersion += " "
		}},
		{"opaque-version-quotes", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.ObjectVersion = `"` + c.ObjectVersion + `"`
		}},
		{"descriptor-version", multipart, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.Descriptor.ObjectVersion += "-other"
		}},
		{"descriptor-bytes", multipart, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) { c.Descriptor.Bytes++ }},
		{"descriptor-count", multipart, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.Descriptor.PartCount++
		}},
		{"rows", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) { c.Rows++ }},
		{"bytes", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) { c.Bytes++ }},
		{"schema", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.SchemaHash = strings.Repeat("c", 64)
		}},
		{"fingerprint", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.Fingerprint += "-other"
		}},
		{"freshness-seconds", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.RefreshedAt = c.RefreshedAt.Add(time.Second)
		}},
		{"freshness-nanoseconds", nil, func(_ *readerlease.Reference, _ *catalog.ObjectLocation, c *objectCommitted) {
			c.RefreshedAt = c.RefreshedAt.Add(time.Nanosecond)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, location, committed := objectReaderBindingFixture()
			if tc.prepare != nil {
				tc.prepare(&ref, &location, &committed)
			}
			before, err := objectReaderBinding(ref, location, &committed)
			if err != nil {
				t.Fatal(err)
			}
			tc.change(&ref, &location, &committed)
			after, err := objectReaderBinding(ref, location, &committed)
			if err != nil || after.ContentSHA256 == before.ContentSHA256 {
				t.Fatalf("changed identity was not bound: err = %v", err)
			}
		})
	}
}

func TestObjectReaderBindingCanonicalTimeAndNoMutation(t *testing.T) {
	for _, instant := range []time.Time{
		time.Now(), // includes a monotonic reading, unlike serialized metadata
		time.Date(1600, 1, 1, 0, 0, 0, 123, time.UTC), // outside UnixNano range
		time.Date(2500, 1, 1, 0, 0, 0, 987, time.UTC),
	} {
		ref, location, committed := objectReaderBindingFixture()
		objectReaderBindingMultipart(&committed)
		committed.RefreshedAt = instant
		original := committed
		descriptor := *committed.Descriptor
		original.Descriptor = &descriptor
		base, err := objectReaderBinding(ref, location, &committed)
		if err != nil || !reflect.DeepEqual(original, committed) {
			t.Fatalf("binding mutated input or rejected canonical time: %v", err)
		}
		for _, sameInstant := range []time.Time{instant.Round(0), instant.UTC(), instant.In(time.FixedZone("offset", 5*3600+30*60))} {
			committed.RefreshedAt = sameInstant
			got, err := objectReaderBinding(ref, location, &committed)
			if err != nil || got != base {
				t.Fatalf("equivalent instant changed binding: %v", err)
			}
		}
	}
}

func TestObjectReaderBindingStringFraming(t *testing.T) {
	ref, location, committed := objectReaderBindingFixture()
	ref.Tenant, ref.Dataset = "alpha", "bc"
	first, err := objectReaderBinding(ref, location, &committed)
	if err != nil {
		t.Fatal(err)
	}
	// Both pairs concatenate to "alphabc" without length framing.
	ref.Tenant, ref.Dataset = "alphab", "c"
	second, err := objectReaderBinding(ref, location, &committed)
	if err != nil || first.ContentSHA256 == second.ContentSHA256 {
		t.Fatalf("ambiguous string framing: %v", err)
	}
}

func TestObjectReaderBindingRejectsInvalidReferenceAndLocation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*readerlease.Reference, *catalog.ObjectLocation)
	}{
		{"tenant-empty", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Tenant = "" }},
		{"tenant-case", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Tenant = "Analytics" }},
		{"tenant-length", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Tenant = strings.Repeat("a", 33) }},
		{"dataset-start", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Dataset = "1events" }},
		{"dataset-path", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Dataset = "../events" }},
		{"dataset-length", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Dataset = strings.Repeat("a", 64) }},
		{"generation-empty", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Generation = "" }},
		{"generation-case", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Generation = strings.Repeat("A", 32) }},
		{"generation-nonhex", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Generation = strings.Repeat("g", 32) }},
		{"incarnation-empty", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Incarnation = "" }},
		{"incarnation-length", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Incarnation = strings.Repeat("a", 63) }},
		{"incarnation-case", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Incarnation = strings.Repeat("A", 64) }},
		{"incarnation-nonhex", func(r *readerlease.Reference, _ *catalog.ObjectLocation) { r.Incarnation = strings.Repeat("g", 64) }},
		{"provider", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Provider = "other" }},
		{"endpoint-http", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Endpoint = "http://objects.example.test" }},
		{"endpoint-credentials", func(_ *readerlease.Reference, l *catalog.ObjectLocation) {
			l.Endpoint = "https://private:secret@objects.example.test"
		}},
		{"endpoint-path", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Endpoint += "/objects" }},
		{"endpoint-query", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Endpoint += "?token=secret" }},
		{"bucket", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Bucket = "Invalid" }},
		{"prefix-empty", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Prefix = "" }},
		{"prefix-traversal", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Prefix = "kelvo/../other" }},
		{"prefix-length", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Prefix = strings.Repeat("a/", 128) + "a" }},
		{"region", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Region = "" }},
		{"account", func(_ *readerlease.Reference, l *catalog.ObjectLocation) { l.Account = "unexpected" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, location, committed := objectReaderBindingFixture()
			tc.change(&ref, &location)
			binding, err := objectReaderBinding(ref, location, &committed)
			if !errors.Is(err, readerlease.ErrInvalid) || binding != (readerlease.Binding{}) {
				t.Fatalf("invalid reference/location returned %+v, %v", binding, err)
			}
		})
	}
}

func TestObjectReaderBindingRejectsInvalidCommit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		multipart bool
		change    func(*objectCommitted)
	}{
		{"generation", false, func(c *objectCommitted) { c.Generation = "invalid" }},
		{"schema-empty", false, func(c *objectCommitted) { c.SchemaHash = "" }},
		{"schema-case", false, func(c *objectCommitted) { c.SchemaHash = strings.Repeat("A", 64) }},
		{"checksum", false, func(c *objectCommitted) { c.SHA256 = "invalid" }},
		{"fingerprint-empty", false, func(c *objectCommitted) { c.Fingerprint = "" }},
		{"fingerprint-length", false, func(c *objectCommitted) { c.Fingerprint = strings.Repeat("f", storeFingerprintLimit+1) }},
		{"rows-negative", false, func(c *objectCommitted) { c.Rows = -1 }},
		{"rows-minimum-int", false, func(c *objectCommitted) { c.Rows = math.MinInt64 }},
		{"bytes-zero", false, func(c *objectCommitted) { c.Bytes = 0 }},
		{"bytes-negative", false, func(c *objectCommitted) { c.Bytes = math.MinInt64 }},
		{"single-bytes-limit", false, func(c *objectCommitted) { c.Bytes = objectstore.MaxUploadBytes + 1 }},
		{"single-bytes-maximum-int", false, func(c *objectCommitted) { c.Bytes = math.MaxInt64 }},
		{"version-empty", false, func(c *objectCommitted) { c.ObjectVersion = "" }},
		{"version-length", false, func(c *objectCommitted) { c.ObjectVersion = strings.Repeat("v", 4097) }},
		{"version-newline", false, func(c *objectCommitted) { c.ObjectVersion = "v\n1" }},
		{"version-null", false, func(c *objectCommitted) { c.ObjectVersion = "v\x001" }},
		{"time-zero", false, func(c *objectCommitted) { c.RefreshedAt = time.Time{} }},
		{"time-year-zero", false, func(c *objectCommitted) { c.RefreshedAt = time.Date(0, 12, 31, 0, 0, 0, 0, time.UTC) }},
		{"time-year-10000", false, func(c *objectCommitted) { c.RefreshedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"time-offset-underflow", false, func(c *objectCommitted) { c.RefreshedAt = time.Date(1, 1, 1, 0, 0, 0, 0, time.FixedZone("plus", 3600)) }},
		{"time-offset-overflow", false, func(c *objectCommitted) {
			c.RefreshedAt = time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("minus", -3600))
		}},
		{"multipart-schema", true, func(c *objectCommitted) { c.SchemaHash = "" }},
		{"multipart-single-version", true, func(c *objectCommitted) { c.ObjectVersion = "unexpected" }},
		{"multipart-total-limit", true, func(c *objectCommitted) { c.Bytes = 1<<40 + 1 }},
		{"descriptor-version", true, func(c *objectCommitted) { c.Descriptor.ObjectVersion = "" }},
		{"descriptor-version-length", true, func(c *objectCommitted) { c.Descriptor.ObjectVersion = strings.Repeat("v", 4097) }},
		{"descriptor-bytes-zero", true, func(c *objectCommitted) { c.Descriptor.Bytes = 0 }},
		{"descriptor-bytes-negative", true, func(c *objectCommitted) { c.Descriptor.Bytes = math.MinInt64 }},
		{"descriptor-bytes-limit", true, func(c *objectCommitted) { c.Descriptor.Bytes = objectDescriptorLimit + 1 }},
		{"descriptor-parts-zero", true, func(c *objectCommitted) { c.Descriptor.PartCount = 0 }},
		{"descriptor-parts-negative", true, func(c *objectCommitted) { c.Descriptor.PartCount = math.MinInt }},
		{"descriptor-parts-limit", true, func(c *objectCommitted) { c.Descriptor.PartCount = 257 }},
		{"descriptor-parts-maximum-int", true, func(c *objectCommitted) { c.Descriptor.PartCount = math.MaxInt }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, location, committed := objectReaderBindingFixture()
			if tc.multipart {
				objectReaderBindingMultipart(&committed)
			}
			tc.change(&committed)
			binding, err := objectReaderBinding(ref, location, &committed)
			if !errors.Is(err, ErrCorrupt) || binding != (readerlease.Binding{}) {
				t.Fatalf("invalid commit returned %+v, %v", binding, err)
			}
		})
	}
	ref, location, committed := objectReaderBindingFixture()
	if binding, err := objectReaderBinding(ref, location, nil); !errors.Is(err, ErrCorrupt) || binding != (readerlease.Binding{}) {
		t.Fatalf("nil commit returned %+v, %v", binding, err)
	}
	committed.Generation = strings.Repeat("c", 32)
	if binding, err := objectReaderBinding(ref, location, &committed); !errors.Is(err, readerlease.ErrBinding) || binding != (readerlease.Binding{}) {
		t.Fatalf("mismatched generation returned %+v, %v", binding, err)
	}
}

func TestObjectReaderBindingAcceptedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*readerlease.Reference, *objectCommitted)
	}{
		{"reference-lengths", func(r *readerlease.Reference, _ *objectCommitted) {
			r.Tenant, r.Dataset = strings.Repeat("a", 32), "_"+strings.Repeat("A", 62)
		}},
		{"empty-dataset", func(_ *readerlease.Reference, c *objectCommitted) { c.Rows = 0 }},
		{"maximum-rows", func(_ *readerlease.Reference, c *objectCommitted) { c.Rows = math.MaxInt64 }},
		{"single-limits", func(_ *readerlease.Reference, c *objectCommitted) {
			c.Bytes, c.ObjectVersion, c.Fingerprint = objectstore.MaxUploadBytes, strings.Repeat("v", 4096), strings.Repeat("f", storeFingerprintLimit)
		}},
		{"multipart-limits", func(_ *readerlease.Reference, c *objectCommitted) {
			objectReaderBindingMultipart(c)
			c.Bytes, c.Descriptor.Bytes, c.Descriptor.PartCount = 1<<40, objectDescriptorLimit, 256
			c.Descriptor.ObjectVersion = strings.Repeat("v", 4096)
		}},
		{"minimum-persisted-time", func(_ *readerlease.Reference, c *objectCommitted) {
			c.RefreshedAt = time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC)
		}},
		{"maximum-persisted-time", func(_ *readerlease.Reference, c *objectCommitted) {
			c.RefreshedAt = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, location, committed := objectReaderBindingFixture()
			tc.change(&ref, &committed)
			binding, err := objectReaderBinding(ref, location, &committed)
			if err != nil || binding.Reference != ref || !storeDigest.MatchString(binding.ContentSHA256) {
				t.Fatalf("valid boundary returned %+v, %v", binding, err)
			}
		})
	}
}
