// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

// MinIOLocation returns the S3-compatible conformance location, or skips.
// Set KELVO_TEST_S3_ENDPOINT (https), KELVO_TEST_S3_BUCKET and the credential
// values in KELVO_SOURCE_TEST_S3_ID / KELVO_SOURCE_TEST_S3_SECRET.
func minioLocation(t *testing.T) (catalog.ObjectLocation, catalog.ObjectCredentials) {
	endpoint, bucket := os.Getenv("KELVO_TEST_S3_ENDPOINT"), os.Getenv("KELVO_TEST_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("S3 conformance endpoint not configured")
	}
	prefix := fmt.Sprintf("conformance/collect-%d", time.Now().UnixNano())
	return catalog.ObjectLocation{Provider: "s3", Endpoint: endpoint, Bucket: bucket, Prefix: prefix, Region: "us-east-1"},
		catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_TEST_S3_ID", SecretAccessKeyEnv: "KELVO_SOURCE_TEST_S3_SECRET"}
}

func TestS3ConformanceListPagesAndDeletesExactVersion(t *testing.T) {
	location, credentials := minioLocation(t)
	client, err := New(location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	collector := client.(Collector)
	ctx := context.Background()
	prefix := location.Prefix + "/ds/"
	versions := map[string]string{}
	for i := range 5 {
		key := fmt.Sprintf("%sobj-%d", prefix, i)
		data := []byte(key)
		sum := sha256.Sum256(data)
		info, err := client.Put(ctx, key, bytes.NewReader(data), int64(len(data)), hex.EncodeToString(sum[:]), Condition{Absent: true})
		if err != nil {
			t.Fatal(err)
		}
		versions[key] = info.Version
	}
	var seen []string
	after := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("listing did not terminate")
		}
		page, err := collector.List(ctx, prefix, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) > 2 {
			t.Fatal("page bound exceeded")
		}
		for _, e := range page.Entries {
			seen = append(seen, e.Key)
			if e.Size != int64(len(e.Key)) {
				t.Fatal("listed size mismatch", e)
			}
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	if len(seen) != 5 {
		t.Fatal("paged listing incomplete", seen)
	}
	if _, err := collector.List(ctx, "elsewhere/", "", 2); err == nil {
		t.Fatal("listing outside the namespace was allowed")
	}
	key := prefix + "obj-0"
	if err := collector.Delete(ctx, key, `"0000000000000000000000000000000a"`); !errors.Is(err, ErrConflict) {
		t.Fatal("delete with a stale version must not delete", err)
	}
	if _, err := client.Head(ctx, key, ""); err != nil {
		t.Fatal("conflicting delete removed the object", err)
	}
	if err := collector.Delete(ctx, key, versions[key]); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Head(ctx, key, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal("exact delete left the object", err)
	}
	if err := collector.Delete(ctx, key, versions[key]); err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal("repeated delete must be idempotent", err)
	}
	for k, v := range versions {
		_ = collector.Delete(ctx, k, v)
	}
}
