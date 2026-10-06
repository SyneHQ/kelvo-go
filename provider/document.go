package provider

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// Provider documents may contain case-sensitive user field names. Only exact
// duplicate keys are invalid; decoding must preserve their numeric text.
func DecodeDocument(raw []byte, destination any, limit int) error {
	if limit < 1 || limit > 8<<20 || len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) {
		return operations.ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return operations.ErrInvalid
		}
		token, err := d.Token()
		if err != nil {
			return operations.ErrInvalid
		}
		open, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		if open != '{' && open != '[' {
			return operations.ErrInvalid
		}
		seen := map[string]bool{}
		for d.More() {
			if open == '{' {
				key, err := d.Token()
				if err != nil {
					return operations.ErrInvalid
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return operations.ErrInvalid
				}
				seen[name] = true
			}
			if err := walk(depth + 1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || open == '{' && end != json.Delim('}') || open == '[' && end != json.Delim(']') {
			return operations.ErrInvalid
		}
		return nil
	}
	if walk(0) != nil {
		return operations.ErrInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return operations.ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(destination) != nil {
		return operations.ErrInvalid
	}
	return nil
}
