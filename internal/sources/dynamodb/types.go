// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package dynamodb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

var numberText = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// Validate the AWS tagged union without constructing a second document tree.
// Output uses the original bytes: no numbers, type tags, nulls or sets are lost.
func validDocument(raw json.RawMessage) bool {
	if !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if !readMap(d, 0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}
func delimiter(d *json.Decoder, want json.Delim) bool {
	token, err := d.Token()
	return err == nil && token == want
}
func readMap(d *json.Decoder, depth int) bool {
	if depth > 32 || !delimiter(d, '{') {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return false
		}
		name, ok := key.(string)
		if !ok || name == "" || len(name) > 65535 || seen[name] {
			return false
		}
		seen[name] = true
		if !readAttribute(d, depth) {
			return false
		}
	}
	return delimiter(d, '}')
}
func readAttribute(d *json.Decoder, depth int) bool {
	if depth > 32 || !delimiter(d, '{') || !d.More() {
		return false
	}
	token, err := d.Token()
	if err != nil {
		return false
	}
	tag, ok := token.(string)
	if !ok {
		return false
	}
	switch tag {
	case "S", "N", "B", "BOOL", "NULL":
		if !readScalar(d, tag) {
			return false
		}
	case "M":
		if !readMap(d, depth+1) {
			return false
		}
	case "L":
		if !delimiter(d, '[') {
			return false
		}
		for d.More() {
			if !readAttribute(d, depth+1) {
				return false
			}
		}
		if !delimiter(d, ']') {
			return false
		}
	case "SS", "NS", "BS":
		if !delimiter(d, '[') || !d.More() {
			return false
		}
		for d.More() {
			if !readScalar(d, tag[:1]) {
				return false
			}
		}
		if !delimiter(d, ']') {
			return false
		}
	default:
		return false
	}
	// An AttributeValue contains exactly one tag; duplicate tags fail here too.
	return !d.More() && delimiter(d, '}')
}
func readScalar(d *json.Decoder, tag string) bool {
	token, err := d.Token()
	if err != nil {
		return false
	}
	switch tag {
	case "BOOL":
		_, ok := token.(bool)
		return ok
	case "NULL":
		value, ok := token.(bool)
		return ok && value
	case "S":
		_, ok := token.(string)
		return ok
	case "N":
		s, ok := token.(string)
		return ok && len(s) <= 256 && numberText.MatchString(s)
	case "B":
		s, ok := token.(string)
		if !ok {
			return false
		}
		_, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(s)))
		return err == nil
	}
	return false
}
