// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

const objectReaderBindingDomain = "kelvo/object-reader-binding"
const objectReaderBindingVersion uint32 = 1

// objectReaderBinding binds verified immutable commit metadata to its registry
// reference and authorized storage location. The caller must verify the object
// or descriptor before sealing, and separately enforce current authorization.
// This helper neither reads storage nor grants access or registers a lease.
// Legacy commits without a schema hash cannot enter the protected format.
// Freshness must be a nonzero instant within UTC years 1 through 9999.
func objectReaderBinding(ref readerlease.Reference, location catalog.ObjectLocation, committed *objectCommitted) (readerlease.Binding, error) {
	if !storeTenantID.MatchString(ref.Tenant) || !storeDatasetID.MatchString(ref.Dataset) ||
		!storeGenerationID.MatchString(ref.Generation) || !storeDigest.MatchString(ref.Incarnation) {
		return readerlease.Binding{}, readerlease.ErrInvalid
	}
	if err := location.Validate(); err != nil {
		return readerlease.Binding{}, readerlease.ErrInvalid
	}
	if err := validateObjectCommit(committed); err != nil {
		return readerlease.Binding{}, err
	}
	if !storeDigest.MatchString(committed.SchemaHash) {
		return readerlease.Binding{}, fmt.Errorf("%w: reader binding requires an immutable schema hash", ErrCorrupt)
	}
	if committed.Generation != ref.Generation {
		return readerlease.Binding{}, readerlease.ErrBinding
	}
	// UTC removes location and monotonic-clock representation. Whole seconds plus
	// nanoseconds avoid UnixNano overflow and retain the entire persisted instant.
	refreshed := committed.RefreshedAt.UTC()
	if refreshed.Year() < 1 || refreshed.Year() > 9999 {
		return readerlease.Binding{}, fmt.Errorf("%w: reader binding freshness is outside the persisted time range", ErrCorrupt)
	}

	// V1 is deliberately independent of Go struct layout and YAML formatting.
	// Strings are raw bytes with a big-endian uint32 byte length. Integers have
	// fixed widths in big-endian order. The exact field order is:
	// domain, version(u32), provider, endpoint, bucket, prefix, region, account,
	// tenant, dataset, generation, incarnation, kind(u8: 1=file, 2=descriptor),
	// checksum, opaque version, descriptor bytes(u64), descriptor count(u32),
	// rows(u64), bytes(u64), schema hash, fingerprint, UTC Unix seconds(i64 in
	// two's complement), nanoseconds(u32). Single files encode zero descriptor
	// bytes/count. Provider aliases and endpoint spelling are not normalized:
	// changing the authorized configuration must change the binding.
	// Adding immutable identity fields requires a new encoding version.
	encoded := make([]byte, 0, 1024)
	appendString := func(value string) {
		encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(value)))
		encoded = append(encoded, value...)
	}
	appendString(objectReaderBindingDomain)
	encoded = binary.BigEndian.AppendUint32(encoded, objectReaderBindingVersion)
	for _, value := range []string{location.Provider, location.Endpoint, location.Bucket, location.Prefix, location.Region, location.Account,
		ref.Tenant, ref.Dataset, ref.Generation, ref.Incarnation} {
		appendString(value)
	}
	kind, version, descriptorBytes, descriptorCount := byte(1), committed.ObjectVersion, int64(0), 0
	if descriptor := committed.Descriptor; descriptor != nil {
		kind, version, descriptorBytes, descriptorCount = 2, descriptor.ObjectVersion, descriptor.Bytes, descriptor.PartCount
	}
	encoded = append(encoded, kind)
	appendString(committed.SHA256)
	appendString(version)
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(descriptorBytes))
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(descriptorCount))
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(committed.Rows))
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(committed.Bytes))
	appendString(committed.SchemaHash)
	appendString(committed.Fingerprint)
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(refreshed.Unix()))
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(refreshed.Nanosecond()))
	digest := sha256.Sum256(encoded)
	return readerlease.Binding{Reference: ref, ContentSHA256: hex.EncodeToString(digest[:])}, nil
}
