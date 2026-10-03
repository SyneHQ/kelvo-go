// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

const objectDescriptorLimit int64 = 2 << 20

// Root manifests retain only this bounded immutable descriptor reference.
// Keys are always derived from the authorized dataset and generation identity.
type objectDescriptorRef struct {
	ObjectVersion string `yaml:"object_version"`
	Bytes         int64  `yaml:"bytes"`
	PartCount     int    `yaml:"part_count"`
}
type objectGenerationDescriptor struct {
	Version    int          `yaml:"version"`
	Dataset    string       `yaml:"dataset"`
	Generation string       `yaml:"generation"`
	SchemaHash string       `yaml:"schema_hash"`
	Rows       int64        `yaml:"rows"`
	Bytes      int64        `yaml:"bytes"`
	Parts      []objectPart `yaml:"parts"`
}
type objectPart struct {
	Rows          int64  `yaml:"rows"`
	Bytes         int64  `yaml:"bytes"`
	SHA256        string `yaml:"sha256"`
	ObjectVersion string `yaml:"object_version"`
}

func objectDescriptorName(generation string) string { return generation + ".parts.yaml" }
