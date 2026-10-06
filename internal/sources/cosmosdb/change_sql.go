// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cosmosdb

// This is a deliberately small SQL-to-REST grammar, implemented independently
// from the legacy driver. Cosmos SQL itself has no mutation statements.
import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type changePlan struct {
	verb, database, container string
	databaseDDL, conditional  bool
	fields                    []string
	values, where             map[string]json.RawMessage
	partition                 []string
	throughput, autoscale     string
}

type changeParser struct {
	text string
	pos  int
	err  error
}

var fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$-]{0,127}$`)

func changeSyntax() error {
	return query.NewError("UNSUPPORTED", "Cosmos mutation requires supported point-write or scoped CREATE/DROP syntax")
}

func (p *changeParser) space() {
	for p.pos < len(p.text) {
		switch p.text[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			if strings.HasPrefix(p.text[p.pos:], "--") {
				for p.pos < len(p.text) && p.text[p.pos] != '\n' {
					p.pos++
				}
			} else if strings.HasPrefix(p.text[p.pos:], "/*") {
				end := strings.Index(p.text[p.pos+2:], "*/")
				if end < 0 {
					p.err = changeSyntax()
					p.pos = len(p.text)
					return
				}
				p.pos += end + 4
			} else {
				return
			}
		}
	}
}
func (p *changeParser) take(s string) bool {
	p.space()
	if p.pos < len(p.text) && strings.HasPrefix(p.text[p.pos:], s) {
		p.pos += len(s)
		return true
	}
	return false
}
func (p *changeParser) word() string {
	p.space()
	start := p.pos
	for p.pos < len(p.text) {
		c := p.text[p.pos]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '$') {
			break
		}
		p.pos++
	}
	return p.text[start:p.pos]
}
func (p *changeParser) keyword(s string) bool {
	position := p.pos
	if strings.EqualFold(p.word(), s) {
		return true
	}
	p.pos = position
	return false
}
func (p *changeParser) require(s string) {
	if !p.keyword(s) {
		p.err = changeSyntax()
	}
}
func (p *changeParser) symbol(s string) {
	if !p.take(s) {
		p.err = changeSyntax()
	}
}
func (p *changeParser) field(nested bool) string {
	name := p.word()
	if !fieldName.MatchString(name) {
		p.err = changeSyntax()
		return ""
	}
	if nested {
		for p.take(".") {
			part := p.word()
			if !fieldName.MatchString(part) || len(name)+len(part) > 1024 {
				p.err = changeSyntax()
				break
			}
			name += "." + part
		}
	}
	return name
}
func (p *changeParser) target(plan *changePlan) {
	first := p.word()
	if !component.MatchString(first) || first == "." || first == ".." {
		p.err = changeSyntax()
	}
	if plan.databaseDDL {
		plan.database = first
		return
	}
	plan.container = first
	if p.take(".") {
		plan.database, plan.container = first, p.word()
		if !component.MatchString(plan.container) {
			p.err = changeSyntax()
		}
	}
}

// value preserves JSON number spelling. Double-quoted values retain the old
// driver's JSON-envelope syntax; ordinary strings use SQL single quotes.
func (p *changeParser) value() json.RawMessage {
	p.space()
	if p.pos == len(p.text) {
		p.err = changeSyntax()
		return nil
	}
	if p.text[p.pos] == '\'' {
		p.pos++
		var value strings.Builder
		for p.pos < len(p.text) {
			c := p.text[p.pos]
			p.pos++
			if c == '\'' {
				if p.pos < len(p.text) && p.text[p.pos] == '\'' {
					p.pos++
					value.WriteByte(c)
					continue
				}
				out, _ := json.Marshal(value.String())
				return out
			}
			value.WriteByte(c)
		}
		p.err = changeSyntax()
		return nil
	}
	for _, keyword := range []string{"null", "true", "false"} {
		if p.keyword(keyword) {
			return json.RawMessage(keyword)
		}
	}
	p.space()
	decoder := json.NewDecoder(strings.NewReader(p.text[p.pos:]))
	var raw json.RawMessage
	if decoder.Decode(&raw) != nil {
		p.err = changeSyntax()
		return nil
	}
	p.pos += int(decoder.InputOffset())
	if len(raw) > 0 && raw[0] == '"' {
		var envelope string
		if json.Unmarshal(raw, &envelope) != nil {
			p.err = changeSyntax()
			return nil
		}
		raw = json.RawMessage(envelope)
	}
	if _, err := strictChangeValue(raw); err != nil {
		p.err = changeSyntax()
		return nil
	}
	return raw
}

func (p *changeParser) paths() []string {
	paths := []string{}
	for p.err == nil {
		path := ""
		for p.take("/") {
			part := p.field(false)
			path += "/" + part
			if len(path) > 1024 {
				p.err = changeSyntax()
				break
			}
		}
		if !validPartitionPath(path) || len(paths) >= 3 {
			p.err = changeSyntax()
			break
		}
		for _, old := range paths {
			if old == path {
				p.err = changeSyntax()
			}
		}
		paths = append(paths, path)
		if !p.take(",") {
			break
		}
	}
	return paths
}
func (p *changeParser) options(plan *changePlan) {
	seen := map[string]bool{}
	for p.keyword("WITH") && p.err == nil {
		key := strings.ToUpper(p.word())
		p.symbol("=")
		if seen[key] {
			p.err = changeSyntax()
			return
		}
		seen[key] = true
		switch key {
		case "PK":
			if plan.databaseDDL || plan.verb == "DROP" {
				p.err = changeSyntax()
				return
			}
			plan.partition = p.paths()
		case "RU", "MAXRU":
			if plan.verb != "CREATE" || plan.throughput != "" || plan.autoscale != "" {
				p.err = changeSyntax()
				return
			}
			value := p.word()
			if !positiveInteger.MatchString(value) || len(value) > 9 {
				p.err = changeSyntax()
			}
			if key == "RU" {
				plan.throughput = value
			} else {
				plan.autoscale = value
			}
		default:
			p.err = changeSyntax()
		}
	}
}
func parseChange(text string) (changePlan, error) {
	plan := changePlan{values: map[string]json.RawMessage{}, where: map[string]json.RawMessage{}}
	if len(text) == 0 || len(text) > 1<<20 || !utf8.ValidString(text) || strings.ContainsRune(text, '\x00') {
		return plan, changeSyntax()
	}
	p := changeParser{text: text}
	plan.verb = strings.ToUpper(p.word())
	switch plan.verb {
	case "INSERT", "UPSERT":
		p.require("INTO")
		p.target(&plan)
		p.symbol("(")
		for p.err == nil {
			field := p.field(false)
			if _, duplicate := plan.values[field]; duplicate || len(plan.fields) >= 256 {
				p.err = changeSyntax()
				break
			}
			plan.values[field] = nil
			plan.fields = append(plan.fields, field)
			if !p.take(",") {
				break
			}
		}
		p.symbol(")")
		p.require("VALUES")
		p.symbol("(")
		for i, field := range plan.fields {
			if i > 0 {
				p.symbol(",")
			}
			plan.values[field] = p.value()
		}
		p.symbol(")")
		p.options(&plan)
	case "UPDATE", "DELETE":
		if plan.verb == "DELETE" {
			p.require("FROM")
		}
		p.target(&plan)
		if plan.verb == "UPDATE" {
			p.require("SET")
			for p.err == nil {
				field := p.field(false)
				p.symbol("=")
				if _, duplicate := plan.values[field]; duplicate || field == "id" || strings.HasPrefix(field, "_") || len(plan.fields) >= 10 {
					p.err = changeSyntax()
					break
				}
				plan.fields = append(plan.fields, field)
				plan.values[field] = p.value()
				if !p.take(",") {
					break
				}
			}
		}
		p.require("WHERE")
		for p.err == nil {
			field := p.field(true)
			p.symbol("=")
			if _, duplicate := plan.where[field]; duplicate || len(plan.where) >= 4 {
				p.err = changeSyntax()
				break
			}
			plan.where[field] = p.value()
			if !p.keyword("AND") {
				break
			}
		}
	case "CREATE", "DROP":
		kind := strings.ToUpper(p.word())
		if kind != "DATABASE" && kind != "TABLE" && kind != "COLLECTION" {
			p.err = changeSyntax()
		}
		plan.databaseDDL = kind == "DATABASE"
		if p.keyword("IF") {
			plan.conditional = true
			if plan.verb == "CREATE" {
				p.require("NOT")
			}
			p.require("EXISTS")
		}
		p.target(&plan)
		if plan.verb == "CREATE" {
			p.options(&plan)
			if !plan.databaseDDL && len(plan.partition) == 0 {
				p.err = changeSyntax()
			}
		}
	default:
		p.err = changeSyntax()
	}
	p.take(";")
	p.space()
	if p.err != nil || p.pos != len(text) {
		return plan, changeSyntax()
	}
	if plan.verb == "INSERT" || plan.verb == "UPSERT" {
		if _, ok := plan.values["id"]; !ok {
			return plan, query.NewError("INVALID_ARGUMENT", "Cosmos documents require an explicit string id")
		}
		for field := range plan.values {
			if strings.HasPrefix(field, "_") {
				return plan, changeSyntax()
			}
		}
		if _, err := documentID(plan.values["id"]); err != nil {
			return plan, err
		}
	}
	if plan.verb == "DELETE" || plan.verb == "UPDATE" {
		if _, err := documentID(plan.where["id"]); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func validPartitionPath(path string) bool {
	if !strings.HasPrefix(path, "/") || len(path) > 1024 {
		return false
	}
	for _, part := range strings.Split(path[1:], "/") {
		if !fieldName.MatchString(part) {
			return false
		}
	}
	return true
}
func documentID(raw json.RawMessage) (string, error) {
	var id string
	if json.Unmarshal(raw, &id) != nil || len(id) == 0 || len(id) > 1023 || id == "." || id == ".." || strings.ContainsAny(id, "/\\?#") || !utf8.ValidString(id) {
		return "", query.NewError("INVALID_ARGUMENT", "Cosmos point writes require a valid string id")
	}
	for _, c := range id {
		if c < 0x20 || c == 0x7f {
			return "", query.NewError("INVALID_ARGUMENT", "Cosmos point writes require a valid string id")
		}
	}
	return id, nil
}

// Reject duplicate object keys and excessive nesting before a partition value
// is extracted. This keeps routing and the transmitted document identical.
func strictChangeValue(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, changeSyntax()
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if depth > 64 || nodes > 16384 {
			return nil, changeSyntax()
		}
		token, err := d.Token()
		if err != nil {
			return nil, changeSyntax()
		}
		switch token {
		case json.Delim('{'):
			object := map[string]any{}
			for d.More() {
				key, err := d.Token()
				name, ok := key.(string)
				if err != nil || !ok {
					return nil, changeSyntax()
				}
				if _, duplicate := object[name]; duplicate {
					return nil, changeSyntax()
				}
				value, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				object[name] = value
			}
			if end, err := d.Token(); err != nil || end != json.Delim('}') {
				return nil, changeSyntax()
			}
			return object, nil
		case json.Delim('['):
			array := []any{}
			for d.More() {
				value, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			if end, err := d.Token(); err != nil || end != json.Delim(']') {
				return nil, changeSyntax()
			}
			return array, nil
		}
		return token, nil
	}
	value, err := read(0)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, changeSyntax()
	}
	return value, nil
}
