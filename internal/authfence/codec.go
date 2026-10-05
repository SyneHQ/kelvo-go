// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"bytes"
	"io"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// EncodeRecord produces generated canonical YAML, not an operator-editable key
// file. The record contains only membership and private-file revision/digest.
func EncodeRecord(record Record) ([]byte, error) {
	if !record.Valid() {
		return nil, ErrInvalid
	}
	var out strings.Builder
	out.WriteString("version: 1\nscope: " + strconv.Quote(record.scope.id) + "\ntenants:\n")
	for _, tenant := range record.scope.tenants {
		out.WriteString("  - " + strconv.Quote(tenant) + "\n")
	}
	out.WriteString("gateways:\n")
	for _, gateway := range record.scope.gateways {
		out.WriteString("  - " + strconv.Quote(gateway) + "\n")
	}
	out.WriteString("revision: " + strconv.FormatUint(record.document.revision, 10) + "\ndocument_sha256: " + strconv.Quote(record.document.SHA256()) + "\n")
	if out.Len() > MaxRecordBytes {
		return nil, ErrInvalid
	}
	return []byte(out.String()), nil
}

// DecodeRecord accepts only exact canonical bytes and never confers authority.
// All errors are static: malformed payloads and YAML errors cannot leak to logs.
func DecodeRecord(raw []byte) (Record, error) {
	if len(raw) == 0 || len(raw) > MaxRecordBytes {
		return Record{}, ErrInvalid
	}
	var tree yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&tree) != nil || decoder.Decode(new(yaml.Node)) != io.EOF || tree.Kind != yaml.DocumentNode || len(tree.Content) != 1 {
		return Record{}, ErrInvalid
	}
	count := 0
	if !plainRecordNode(&tree, 0, &count) {
		return Record{}, ErrInvalid
	}
	fields, ok := recordFields(tree.Content[0])
	if !ok || !recordScalar(fields["version"], "!!int") || fields["version"].Value != "1" ||
		!recordScalar(fields["scope"], "!!str") || !recordScalar(fields["revision"], "!!int") || !recordScalar(fields["document_sha256"], "!!str") {
		return Record{}, ErrInvalid
	}
	tenants, ok := recordRoster(fields["tenants"], MaxTenants, false)
	if !ok {
		return Record{}, ErrInvalid
	}
	gateways, ok := recordRoster(fields["gateways"], MaxGateways, true)
	if !ok {
		return Record{}, ErrInvalid
	}
	scope, err := NewScope(fields["scope"].Value, tenants, gateways)
	if err != nil {
		return Record{}, ErrInvalid
	}
	revision, err := strconv.ParseUint(fields["revision"].Value, 10, 64)
	if err != nil || strconv.FormatUint(revision, 10) != fields["revision"].Value {
		return Record{}, ErrInvalid
	}
	document, err := NewDocument(revision, fields["document_sha256"].Value)
	if err != nil {
		return Record{}, ErrInvalid
	}
	record, err := NewRecord(scope, document)
	if err != nil {
		return Record{}, ErrInvalid
	}
	canonical, err := EncodeRecord(record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Record{}, ErrInvalid
	}
	return record, nil
}

func plainRecordNode(node *yaml.Node, depth int, count *int) bool {
	*count++
	if depth > 3 || *count > MaxTenants+MaxGateways+16 || node.Kind == yaml.AliasNode || node.Anchor != "" || node.Value == "<<" ||
		node.HeadComment != "" || node.LineComment != "" || node.FootComment != "" {
		return false
	}
	for _, child := range node.Content {
		if !plainRecordNode(child, depth+1, count) {
			return false
		}
	}
	return true
}

func recordFields(node *yaml.Node) (map[string]*yaml.Node, bool) {
	names := []string{"version", "scope", "tenants", "gateways", "revision", "document_sha256"}
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content) != 2*len(names) {
		return nil, false
	}
	result := make(map[string]*yaml.Node, len(names))
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if !recordScalar(key, "!!str") || !slices.Contains(names, key.Value) || result[key.Value] != nil {
			return nil, false
		}
		result[key.Value] = node.Content[i+1]
	}
	return result, true
}

func recordScalar(node *yaml.Node, tag string) bool {
	return node != nil && node.Kind == yaml.ScalarNode && node.Tag == tag
}

func recordRoster(node *yaml.Node, maximum int, gateways bool) ([]string, bool) {
	if node == nil || node.Kind != yaml.SequenceNode || node.Tag != "!!seq" || len(node.Content) == 0 || len(node.Content) > maximum {
		return nil, false
	}
	result := make([]string, len(node.Content))
	for i, child := range node.Content {
		if !recordScalar(child, "!!str") {
			return nil, false
		}
		result[i] = child.Value
	}
	return result, validRoster(result, maximum, gateways)
}
