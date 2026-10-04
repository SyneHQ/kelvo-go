// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"bytes"
	"encoding/hex"
	"io"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

const stateLimit = 2 << 20

// The generated state is canonical YAML, not an operator-editable key file.
func encodeState(scope Scope, state Candidate) ([]byte, error) {
	var out strings.Builder
	out.WriteString("version: 1\nscope: " + strconv.Quote(scope.ID) + "\ntenants:\n")
	for _, tenant := range scope.Tenants {
		out.WriteString("  - " + strconv.Quote(tenant) + "\n")
	}
	out.WriteString("revision: " + strconv.FormatUint(state.Revision, 10) + "\ndocument_sha256: " + strconv.Quote(hex.EncodeToString(state.DocumentSHA256[:])) + "\n")
	if len(state.Owners) == 0 {
		out.WriteString("owners: []\n")
	} else {
		out.WriteString("owners:\n")
		fingerprints := make([][32]byte, 0, len(state.Owners))
		for fingerprint := range state.Owners {
			fingerprints = append(fingerprints, fingerprint)
		}
		slices.SortFunc(fingerprints, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
		for _, fingerprint := range fingerprints {
			owner := state.Owners[fingerprint]
			out.WriteString("  - fingerprint: " + strconv.Quote(hex.EncodeToString(fingerprint[:])) + "\n    tenant_id: " + strconv.Quote(owner.TenantID) + "\n    principal_id: " + strconv.Quote(owner.PrincipalID) + "\n")
		}
	}
	if out.Len() > stateLimit {
		return nil, ErrRejected
	}
	return []byte(out.String()), nil
}

func plainState(node *yaml.Node, depth int, count *int) bool {
	*count++
	if depth > 8 || *count > 100000 || node.Kind == yaml.AliasNode || node.Anchor != "" || node.Value == "<<" {
		return false
	}
	for _, child := range node.Content {
		if !plainState(child, depth+1, count) {
			return false
		}
	}
	return true
}

func mapping(node *yaml.Node, names ...string) (map[string]*yaml.Node, bool) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content) != 2*len(names) {
		return nil, false
	}
	result := make(map[string]*yaml.Node, len(names))
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || !slices.Contains(names, key.Value) || result[key.Value] != nil {
			return nil, false
		}
		result[key.Value] = node.Content[i+1]
	}
	return result, true
}

func scalar(node *yaml.Node, tag string) bool {
	return node != nil && node.Kind == yaml.ScalarNode && node.Tag == tag
}

func digestNode(node *yaml.Node) ([32]byte, bool) {
	var result [32]byte
	if !scalar(node, "!!str") || len(node.Value) != 64 {
		return result, false
	}
	decoded, err := hex.DecodeString(node.Value)
	if err != nil || hex.EncodeToString(decoded) != node.Value {
		return result, false
	}
	copy(result[:], decoded)
	return result, true
}

func decodeState(raw []byte, expected Scope) (Candidate, error) {
	bad := func() (Candidate, error) { return Candidate{}, ErrUnavailable }
	if len(raw) == 0 || len(raw) > stateLimit {
		return bad()
	}
	var tree yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&tree) != nil || decoder.Decode(new(yaml.Node)) != io.EOF || tree.Kind != yaml.DocumentNode || len(tree.Content) != 1 {
		return bad()
	}
	count := 0
	if !plainState(&tree, 0, &count) {
		return bad()
	}
	fields, ok := mapping(tree.Content[0], "version", "scope", "tenants", "revision", "document_sha256", "owners")
	if !ok || !scalar(fields["version"], "!!int") || fields["version"].Value != "1" || !scalar(fields["scope"], "!!str") || fields["scope"].Value != expected.ID {
		return bad()
	}
	tenants := fields["tenants"]
	if tenants.Kind != yaml.SequenceNode || tenants.Tag != "!!seq" || len(tenants.Content) != len(expected.Tenants) {
		return bad()
	}
	for i, node := range tenants.Content {
		if !scalar(node, "!!str") || node.Value != expected.Tenants[i] {
			return bad()
		}
	}
	if !scalar(fields["revision"], "!!int") {
		return bad()
	}
	revision, err := strconv.ParseUint(fields["revision"].Value, 10, 64)
	if err != nil || revision == 0 || strconv.FormatUint(revision, 10) != fields["revision"].Value {
		return bad()
	}
	digest, ok := digestNode(fields["document_sha256"])
	if !ok {
		return bad()
	}
	owners := fields["owners"]
	if owners.Kind != yaml.SequenceNode || owners.Tag != "!!seq" || len(owners.Content) > MaxOwners {
		return bad()
	}
	state := Candidate{Revision: revision, DocumentSHA256: digest, Owners: make(map[[32]byte]Owner, len(owners.Content))}
	var prior [32]byte
	for i, node := range owners.Content {
		owner, valid := mapping(node, "fingerprint", "tenant_id", "principal_id")
		if !valid || !scalar(owner["tenant_id"], "!!str") || !scalar(owner["principal_id"], "!!str") {
			return bad()
		}
		fingerprint, valid := digestNode(owner["fingerprint"])
		if !valid || (i != 0 && bytes.Compare(prior[:], fingerprint[:]) >= 0) {
			return bad()
		}
		state.Owners[fingerprint] = Owner{owner["tenant_id"].Value, owner["principal_id"].Value}
		prior = fingerprint
	}
	state, err = copyCandidate(state, expected)
	if err != nil {
		return bad()
	}
	canonical, err := encodeState(expected, state)
	if err != nil || !bytes.Equal(canonical, raw) {
		return bad()
	}
	return state, nil
}
