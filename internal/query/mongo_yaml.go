// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import (
	"encoding/json"
	"errors"
	"math/big"
	"strings"

	"go.yaml.in/yaml/v3"
)

// UnmarshalYAML preserves numeric lexemes while converting configured pipeline
// stages to the existing Extended JSON request contract. In particular, it must
// never decode large integers through float64.
func (m *MongoRequest) UnmarshalYAML(node *yaml.Node) error {
	bad := errors.New("MongoDB YAML requires collection and a sequence of pipeline objects")
	if node.Kind != yaml.MappingNode {
		return bad
	}
	var out MongoRequest
	seen := map[string]bool{}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Tag != "!!str" || seen[key.Value] {
			return bad
		}
		seen[key.Value] = true
		switch key.Value {
		case "collection":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
				return bad
			}
			out.Collection = value.Value
		case "pipeline":
			if value.Kind != yaml.SequenceNode || len(value.Content) > 128 {
				return bad
			}
			out.Pipeline = make([]json.RawMessage, 0, len(value.Content))
			for _, stage := range value.Content {
				if stage.Kind != yaml.MappingNode {
					return bad
				}
				budget := 8192
				value, err := yamlJSONValue(stage, 0, &budget)
				if err != nil {
					return err
				}
				encoded, err := json.Marshal(value)
				if err != nil || len(encoded) > 128<<10 {
					return bad
				}
				out.Pipeline = append(out.Pipeline, encoded)
			}
		default:
			return bad
		}
	}
	if !seen["collection"] || !seen["pipeline"] {
		return bad
	}
	*m = out
	return nil
}

func yamlJSONValue(n *yaml.Node, depth int, budget *int) (any, error) {
	bad := errors.New("MongoDB pipeline YAML must contain bounded JSON-compatible values without aliases or duplicate keys")
	*budget--
	if depth > 32 || *budget < 0 || n.Kind == yaml.AliasNode || n.Anchor != "" {
		return nil, bad
	}
	switch n.Kind {
	case yaml.MappingNode:
		m := map[string]any{}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" {
				return nil, bad
			}
			if _, exists := m[k.Value]; exists {
				return nil, bad
			}
			v, err := yamlJSONValue(n.Content[i+1], depth+1, budget)
			if err != nil {
				return nil, err
			}
			m[k.Value] = v
		}
		return m, nil
	case yaml.SequenceNode:
		a := make([]any, 0, len(n.Content))
		for _, child := range n.Content {
			v, err := yamlJSONValue(child, depth+1, budget)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		return a, nil
	case yaml.ScalarNode:
		switch n.Tag {
		case "!!str":
			return n.Value, nil
		case "!!null":
			return nil, nil
		case "!!bool":
			switch strings.ToLower(n.Value) {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
		case "!!int":
			v := strings.ReplaceAll(n.Value, "_", "")
			number, ok := new(big.Int).SetString(v, 0)
			if ok {
				return json.Number(number.String()), nil
			}
		case "!!float":
			v := strings.ReplaceAll(n.Value, "_", "")
			v = strings.TrimPrefix(v, "+")
			if strings.HasPrefix(v, ".") {
				v = "0" + v
			} else if strings.HasPrefix(v, "-.") {
				v = "-0" + v[1:]
			}
			if strings.HasSuffix(v, ".") {
				v += "0"
			}
			if json.Valid([]byte(v)) && strings.ContainsAny(v, ".eE") {
				return json.Number(v), nil
			}
		}
	}
	return nil, bad
}

func (m MongoRequest) MarshalYAML() (any, error) {
	sequence := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	for _, raw := range m.Pipeline {
		if !json.Valid(raw) {
			return nil, errors.New("invalid MongoDB pipeline JSON")
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return nil, errors.New("invalid MongoDB pipeline JSON")
		}
		sequence.Content = append(sequence.Content, doc.Content[0])
	}
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: "collection"}, {Kind: yaml.ScalarNode, Tag: "!!str", Value: m.Collection},
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: "pipeline"}, sequence,
	}}, nil
}
