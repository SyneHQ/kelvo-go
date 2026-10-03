// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"time"

	"go.yaml.in/yaml/v3"
)

type entry struct {
	ID        string    `yaml:"id"`
	Owner     string    `yaml:"owner"`
	Sequence  uint64    `yaml:"sequence"`
	RenewedAt time.Time `yaml:"renewed_at"`
	ExpiresAt time.Time `yaml:"expires_at"`
}
type document struct {
	Version       int `yaml:"version"`
	Reference     `yaml:",inline"`
	Sequence      uint64  `yaml:"sequence"`
	WriterOwner   string  `yaml:"writer_owner"`
	Sealed        bool    `yaml:"sealed"`
	ContentSHA256 string  `yaml:"content_sha256,omitempty"`
	Readers       []entry `yaml:"readers,omitempty"`
}

func validateDocument(d document, c Config) error {
	if d.Version != 1 || !validReference(d.Reference) || d.Sequence == 0 ||
		(!hexToken(d.WriterOwner, 32) && !hexToken(d.WriterOwner, 64)) || len(d.Readers) > c.MaxReaders ||
		(d.Sealed && !hexToken(d.ContentSHA256, 64)) || (!d.Sealed && (d.ContentSHA256 != "" || len(d.Readers) != 0)) {
		return ErrCorrupt
	}
	seen := make(map[string]bool, len(d.Readers))
	for _, e := range d.Readers {
		if !hexToken(e.ID, 64) || !hexToken(e.Owner, 64) || seen[e.ID] || e.Sequence == 0 || e.RenewedAt.IsZero() ||
			!e.ExpiresAt.After(e.RenewedAt) || e.ExpiresAt.Sub(e.RenewedAt) > c.LeaseDuration {
			return ErrCorrupt
		}
		seen[e.ID] = true
	}
	return nil
}

func encode(d document, c Config) ([]byte, error) {
	if err := validateDocument(d, c); err != nil {
		return nil, err
	}
	raw, err := yaml.Marshal(d)
	if err != nil {
		return nil, ErrCorrupt
	}
	if len(raw) > c.MaxBytes {
		return nil, ErrCapacity
	}
	return raw, nil
}

func unsignedScalar(n *yaml.Node) bool {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || len(n.Value) == 0 || len(n.Value) > 20 ||
		(len(n.Value) > 1 && n.Value[0] == '0') {
		return false
	}
	for _, char := range n.Value {
		if char < '0' || char > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(n.Value, 10, 64)
	return err == nil
}

// yaml.v3 intentionally permits scalar coercion, including float-to-integer
// truncation. Durable fencing documents require canonical types before decode.
func typedMapping(n *yaml.Node, reader bool) bool {
	if n.Kind != yaml.MappingNode || n.Tag != "!!map" {
		return false
	}
	required := []string{"version", "tenant", "dataset", "generation", "incarnation", "sequence", "writer_owner", "sealed"}
	if reader {
		required = []string{"id", "owner", "sequence", "renewed_at", "expires_at"}
	}
	seen := make(map[string]bool, len(n.Content)/2)
	for i := 0; i < len(n.Content); i += 2 {
		key, value := n.Content[i].Value, n.Content[i+1]
		seen[key] = true
		valid := false
		switch key {
		case "version", "sequence":
			valid = (key == "sequence" || !reader) && unsignedScalar(value)
		case "id", "owner":
			valid = reader && value.Kind == yaml.ScalarNode && value.Tag == "!!str"
		case "tenant", "dataset", "generation", "incarnation", "writer_owner", "content_sha256":
			valid = !reader && value.Kind == yaml.ScalarNode && value.Tag == "!!str"
		case "sealed":
			valid = !reader && value.Kind == yaml.ScalarNode && value.Tag == "!!bool" && (value.Value == "true" || value.Value == "false")
		case "renewed_at", "expires_at":
			parsed, err := time.Parse(time.RFC3339Nano, value.Value)
			valid = reader && value.Kind == yaml.ScalarNode && value.Tag == "!!timestamp" && err == nil && parsed.UTC().Format(time.RFC3339Nano) == value.Value
		case "readers":
			valid = !reader && value.Kind == yaml.SequenceNode && value.Tag == "!!seq"
			if valid {
				for _, child := range value.Content {
					if !typedMapping(child, true) {
						return false
					}
				}
			}
		}
		if !valid {
			return false
		}
	}
	for _, key := range required {
		if !seen[key] {
			return false
		}
	}
	return true
}

func decode(raw []byte, c Config) (document, error) {
	var d document
	if len(raw) == 0 || len(raw) > c.MaxBytes {
		return d, ErrCorrupt
	}
	var tree yaml.Node
	parser := yaml.NewDecoder(bytes.NewReader(raw))
	if parser.Decode(&tree) != nil {
		return d, ErrCorrupt
	}
	var extra any
	if err := parser.Decode(&extra); !errors.Is(err, io.EOF) {
		return d, ErrCorrupt
	}
	count := 0
	var inspect func(*yaml.Node, int) bool
	inspect = func(n *yaml.Node, depth int) bool {
		count++
		if depth > 12 || count > 4096 || n.Kind == yaml.AliasNode || n.Anchor != "" {
			return false
		}
		if n.Kind == yaml.MappingNode {
			keys := map[string]bool{}
			for i := 0; i < len(n.Content); i += 2 {
				key := n.Content[i]
				if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || keys[key.Value] {
					return false
				}
				keys[key.Value] = true
			}
		}
		for _, child := range n.Content {
			if !inspect(child, depth+1) {
				return false
			}
		}
		return true
	}
	if !inspect(&tree, 0) || tree.Kind != yaml.DocumentNode || len(tree.Content) != 1 || !typedMapping(tree.Content[0], false) {
		return d, ErrCorrupt
	}
	parser = yaml.NewDecoder(bytes.NewReader(raw))
	parser.KnownFields(true)
	if parser.Decode(&d) != nil || validateDocument(d, c) != nil {
		return document{}, ErrCorrupt
	}
	return d, nil
}
