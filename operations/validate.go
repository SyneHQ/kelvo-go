// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var ErrInvalid = errors.New("invalid database operation contract")
var ErrUnsupported = errors.New("database operation capability is unsupported")
var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var decimalPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

func text(s string, limit int) bool {
	return s != "" && len(s) <= limit && utf8.ValidString(s) && !strings.ContainsAny(s, "\x00\r\n")
}
func optionalText(s string, limit int) bool { return s == "" || text(s, limit) }
func ValidID(s string) bool                 { return namePattern.MatchString(s) }
func ValidDigest(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (k Kind) Valid() bool {
	switch k {
	case ConnectionTest, MetadataInspect, QueryRead, StatementExecute, NativeRead, NativeExecute,
		SchemaApply, MigrationStatus, MigrationApply, IngestionInstall, IngestionState, IngestionCommit,
		WatchInstall, WatchRemove, WatchRead, WatchAck:
		return true
	}
	return false
}
func (k Kind) Mutating() bool {
	switch k {
	case StatementExecute, NativeExecute, SchemaApply, MigrationApply, IngestionInstall, IngestionCommit, WatchInstall, WatchRemove, WatchAck:
		return true
	}
	return false
}

func (r Request) Validate() error {
	if r.Version != Version || !r.Kind.Valid() || !text(r.Connection.ID, 256) || !optionalText(r.Connection.Database, 256) || !optionalText(r.Connection.Schema, 256) ||
		!optionalText(r.IdempotencyKey, 128) || (r.Kind.Mutating() && r.IdempotencyKey == "") || !optionalText(r.ApprovalID, 256) {
		return ErrInvalid
	}
	count := 0
	for _, present := range []bool{r.Spec.Query != nil, r.Spec.Statement != nil, r.Spec.Metadata != nil, r.Spec.Schema != nil, r.Spec.Migration != nil, r.Spec.Native != nil, r.Spec.Ingestion != nil, r.Spec.Watch != nil} {
		if present {
			count++
		}
	}
	if (r.Kind == ConnectionTest && count != 0) || (r.Kind != ConnectionTest && count != 1) {
		return ErrInvalid
	}
	valid := false
	switch r.Kind {
	case ConnectionTest:
		valid = true
	case QueryRead:
		v := r.Spec.Query
		valid = v != nil && validSQL(v.SQL, v.Parameters)
	case StatementExecute:
		v := r.Spec.Statement
		valid = v != nil && validStatement(*v)
	case MetadataInspect:
		v := r.Spec.Metadata
		if v != nil && validObject(v.Target) && optionalText(v.Cursor, 4096) && v.Limit >= 1 && v.Limit <= 10000 {
			switch v.Object {
			case "catalogs", "databases", "schemas", "tables", "columns", "primary_keys", "foreign_keys", "relationships", "indexes", "types", "ddl", "functions", "procedures", "triggers", "sequences", "extract", "table_summary":
				valid = v.ObjectKind == ""
			case "objects":
				switch v.ObjectKind {
				case "function", "procedure", "trigger", "view", "materialized_view", "package", "package_body":
					valid = v.Cursor == "" && v.Limit <= 500
				}
			}
		}
	case SchemaApply:
		v := r.Spec.Schema
		valid = v != nil && validObject(v.Target) && v.Target.Name != "" && ValidDigest(v.ExpectedSHA256) && v.Plan.Validate() == nil && v.Plan.Format == "schema_plan_v1" && validTransaction(v.Transaction)
	case MigrationApply:
		v := r.Spec.Migration
		valid = v != nil && text(v.ID, 256) && optionalText(v.ExpectedVersion, 256) && v.Plan.Validate() == nil && v.Plan.Format == "migration_plan_v1" && validTransaction(v.Transaction)
	case MigrationStatus:
		v := r.Spec.Migration
		valid = v != nil && v.ID == "schema_migrations" && v.ExpectedVersion == "" && v.Plan == (InputRef{}) && v.Transaction == ""
	case NativeRead, NativeExecute:
		v := r.Spec.Native
		valid = v != nil && ValidID(v.Provider) && ValidID(v.Command) && validParameters(v.Parameters) && (v.Input == nil || v.Input.Validate() == nil) && (!v.ReturnResult || r.Kind == NativeExecute)
	case IngestionInstall, IngestionState, IngestionCommit:
		v := r.Spec.Ingestion
		if v != nil && text(v.Scope.SourceID, 256) && text(v.Scope.Stream, 256) && ValidDigest(v.Scope.Binding) {
			if r.Kind == IngestionCommit {
				valid = text(v.BatchID, 128) && v.ExpectedSequence >= 0 && v.ExpectedSequence < math.MaxInt64 && v.Input != nil && v.Input.Validate() == nil && v.Input.Format == "ingestion_batch_v1"
			} else {
				valid = v.BatchID == "" && v.ExpectedSequence == 0 && v.Input == nil
			}
		}
	case WatchInstall, WatchRemove, WatchRead, WatchAck:
		v := r.Spec.Watch
		if v != nil && text(v.ID, 256) && ValidID(v.Generation) && validObject(v.Target) && v.Target.Name != "" && (v.Mode == "poll" || v.Mode == "native") && (v.Checkpoint == nil || v.Checkpoint.Validate() == nil && v.Checkpoint.Format == "watch_checkpoint_v1") && (v.Resume == nil || r.Kind == WatchInstall && v.Mode == "native" && v.Resume.Validate() == nil && v.Resume.Format == "mongo_watch_resume_v1" && v.Resume.Bytes <= 16<<10) {
			switch r.Kind {
			case WatchRead:
				valid = v.MaxEvents >= 1 && v.MaxEvents <= 10000 && v.MaxWaitMS >= 1 && v.MaxWaitMS <= 60000 && v.SinkReceiptSHA256 == ""
			case WatchAck:
				valid = v.MaxEvents == 0 && v.MaxWaitMS == 0 && v.Checkpoint != nil && ValidDigest(v.SinkReceiptSHA256)
			default:
				valid = v.MaxEvents == 0 && v.MaxWaitMS == 0 && v.Checkpoint == nil && v.SinkReceiptSHA256 == ""
			}
		}
	}
	if !valid {
		return ErrInvalid
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > MaxRequestBytes {
		return ErrInvalid
	}
	return nil
}

func validStatement(v StatementSpec) bool {
	if !validTransaction(v.Transaction) || !optionalText(v.Role, 128) || !validIsolation(v.Isolation) || v.Isolation != "" && v.Transaction != TransactionRequired {
		return false
	}
	if v.Batch == nil {
		return validSQL(v.SQL, v.Parameters)
	}
	if v.SQL != "" || len(v.Parameters) != 0 || len(v.Batch.Statements) < 1 || len(v.Batch.Statements) > 100 {
		return false
	}
	for _, statement := range v.Batch.Statements {
		if !validSQL(statement.SQL, statement.Parameters) {
			return false
		}
	}
	return true
}
func validIsolation(value string) bool {
	switch value {
	case "", "default", "read_uncommitted", "read_committed", "repeatable_read", "serializable":
		return true
	}
	return false
}

func validTransaction(v TransactionMode) bool {
	return v == TransactionRequired || v == TransactionAutocommit
}
func validObject(v ObjectRef) bool {
	return optionalText(v.Catalog, 256) && optionalText(v.Schema, 256) && optionalText(v.Name, 256)
}
func validSQL(sql string, parameters []Parameter) bool {
	return strings.TrimSpace(sql) != "" && len(sql) <= 100000 && utf8.ValidString(sql) && !strings.ContainsRune(sql, 0) && validParameters(parameters)
}
func validParameters(parameters []Parameter) bool {
	if len(parameters) > 1024 {
		return false
	}
	for _, value := range parameters {
		if value.Validate() != nil {
			return false
		}
	}
	return true
}

func (v InputRef) Validate() error {
	if !ValidID(v.ID) || !ValidDigest(v.SHA256) || v.Bytes < 1 || v.Bytes > 1<<40 {
		return ErrInvalid
	}
	switch v.Format {
	case "operation_request_v1", "arrow_ipc", "schema_plan_v1", "migration_plan_v1", "ingestion_batch_v1", "native_input_v1", "watch_checkpoint_v1", "mongo_watch_resume_v1":
		return nil
	}
	return ErrInvalid
}

func parameterType(value string) bool {
	switch value {
	case "null", "string", "bool", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "decimal128", "decimal256", "binary", "date", "timestamp", "json":
		return true
	}
	return false
}

func (p Parameter) Validate() error {
	if !parameterType(p.Type) || len(p.Value) == 0 || len(p.Value) > 16<<10 || !utf8.Valid(p.Value) {
		return ErrInvalid
	}
	var value any
	if DecodeStrict(p.Value, &value, 16<<10) != nil {
		return ErrInvalid
	}
	if p.Type == "null" {
		if !bytes.Equal(bytes.TrimSpace(p.Value), []byte("null")) {
			return ErrInvalid
		}
		return nil
	}
	if value == nil {
		return ErrInvalid
	}
	switch p.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return ErrInvalid
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return ErrInvalid
		}
	case "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64":
		var lexical string
		switch v := value.(type) {
		case string:
			lexical = v
		case json.Number:
			lexical = string(v)
		default:
			return ErrInvalid
		}
		if strings.HasPrefix(p.Type, "uint") {
			bits, _ := strconv.Atoi(strings.TrimPrefix(p.Type, "uint"))
			if _, err := strconv.ParseUint(lexical, 10, bits); err != nil {
				return ErrInvalid
			}
		} else {
			bits, _ := strconv.Atoi(strings.TrimPrefix(p.Type, "int"))
			if _, err := strconv.ParseInt(lexical, 10, bits); err != nil {
				return ErrInvalid
			}
		}
	case "float32", "float64":
		v, ok := value.(json.Number)
		if !ok {
			return ErrInvalid
		}
		bits := 64
		if p.Type == "float32" {
			bits = 32
		}
		n, err := strconv.ParseFloat(string(v), bits)
		if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
			return ErrInvalid
		}
	case "decimal128", "decimal256":
		v, ok := value.(string)
		if !ok || !decimalPattern.MatchString(v) {
			return ErrInvalid
		}
		digits := len(strings.ReplaceAll(strings.TrimPrefix(v, "-"), ".", ""))
		limit := 38
		if p.Type == "decimal256" {
			limit = 76
		}
		if digits > limit {
			return ErrInvalid
		}
	case "binary":
		v, ok := value.(string)
		if !ok {
			return ErrInvalid
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(v)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != v {
			return ErrInvalid
		}
	case "date", "timestamp":
		v, ok := value.(string)
		if !ok {
			return ErrInvalid
		}
		layout := time.RFC3339Nano
		if p.Type == "date" {
			layout = "2006-01-02"
		}
		if _, err := time.Parse(layout, v); err != nil {
			return ErrInvalid
		}
	case "json":
		// Exact validated JSON is carried unchanged; adapters choose supported shapes.
	}
	return nil
}

// DecodeStrict rejects ambiguous control fields, duplicate document keys,
// trailing values, unknown fields, invalid UTF-8 and excessive nesting. Raw JSON
// and map keys remain case-sensitive, as required by customer database records.
func DecodeStrict(raw []byte, destination any, limit int) error {
	if limit < 1 || limit > 3<<20 || len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int, reflect.Type) error
	walk = func(depth int, shape reflect.Type) error {
		if depth > 24 {
			return ErrInvalid
		}
		shape = strictJSONShape(shape)
		token, err := d.Token()
		if err != nil {
			return ErrInvalid
		}
		opening, nested := token.(json.Delim)
		if !nested {
			return nil
		}
		if opening != '{' && opening != '[' {
			return ErrInvalid
		}
		seen := map[string]bool{}
		for d.More() {
			var child reflect.Type
			if shape != nil && (shape.Kind() == reflect.Array || shape.Kind() == reflect.Slice || shape.Kind() == reflect.Map) {
				child = shape.Elem()
			}
			if opening == '{' {
				key, err := d.Token()
				if err != nil {
					return ErrInvalid
				}
				name, ok := key.(string)
				if !ok {
					return ErrInvalid
				}
				if shape != nil && shape.Kind() == reflect.Struct {
					child = strictJSONField(shape, name, 0)
					name = strictJSONKey(name)
				}
				if seen[name] {
					return ErrInvalid
				}
				seen[name] = true
			}
			if walk(depth+1, child) != nil {
				return ErrInvalid
			}
		}
		end, err := d.Token()
		if err != nil || (opening == '{' && end != json.Delim('}')) || (opening == '[' && end != json.Delim(']')) {
			return ErrInvalid
		}
		return nil
	}
	if walk(0, reflect.TypeOf(destination)) != nil {
		return ErrInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if d.Decode(destination) != nil {
		return ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}

func strictJSONShape(shape reflect.Type) reflect.Type {
	for shape != nil && shape.Kind() == reflect.Pointer {
		shape = shape.Elem()
	}
	if shape == reflect.TypeOf(json.RawMessage{}) {
		return nil
	}
	return shape
}

func strictJSONField(shape reflect.Type, name string, depth int) reflect.Type {
	if field := strictJSONFieldMatch(shape, name, depth, true); field != nil {
		return field
	}
	return strictJSONFieldMatch(shape, name, depth, false)
}

func strictJSONFieldMatch(shape reflect.Type, name string, depth int, exact bool) reflect.Type {
	if depth > 24 {
		return nil
	}
	for i := 0; i < shape.NumField(); i++ {
		field := shape.Field(i)
		tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if tag == "-" || field.PkgPath != "" && !field.Anonymous {
			continue
		}
		if tag == "" && field.Anonymous {
			if nested := strictJSONShape(field.Type); nested != nil && nested.Kind() == reflect.Struct {
				if child := strictJSONFieldMatch(nested, name, depth+1, exact); child != nil {
					return child
				}
			}
		}
		if tag == "" {
			tag = field.Name
		}
		if tag == name || !exact && strings.EqualFold(tag, name) {
			return field.Type
		}
	}
	return nil
}

// Match encoding/json's Unicode case folding, including long-s and Kelvin sign.
func strictJSONKey(name string) string {
	return strings.Map(func(r rune) rune {
		canonical := r
		for folded := unicode.SimpleFold(r); folded != r; folded = unicode.SimpleFold(folded) {
			canonical = min(canonical, folded)
		}
		return canonical
	}, name)
}

func ParseRequest(raw []byte) (Request, error) {
	var result Request
	if DecodeStrict(raw, &result, MaxRequestBytes) != nil || result.Validate() != nil {
		return Request{}, ErrInvalid
	}
	return result, nil
}

func (r Receipt) Validate() error {
	if r.Version != Version || !ValidID(r.OperationID) || !ValidDigest(r.RequestSHA256) || !optionalText(r.ProviderReference, 512) || len(r.Steps) > 128 || (r.AffectedRows != nil && *r.AffectedRows < 0) {
		return ErrInvalid
	}
	switch r.Outcome {
	case Completed:
		if (r.Effect != EffectNone && r.Effect != EffectCommitted) || r.ErrorCode != "" {
			return ErrInvalid
		}
	case Rejected, CancelledBeforeStart:
		if r.Effect != EffectNone || r.Result != nil || r.AffectedRows != nil || len(r.Steps) != 0 || r.ErrorCode == "" {
			return ErrInvalid
		}
	case Failed:
		if (r.Effect != EffectNone && r.Effect != EffectPartial) || r.Result != nil || r.ErrorCode == "" {
			return ErrInvalid
		}
	case OutcomeUnknown:
		if r.Effect != EffectUnknown || r.Result != nil || r.ErrorCode == "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if r.ErrorCode != "" {
		switch r.ErrorCode {
		case "INVALID_ARGUMENT", "PERMISSION_DENIED", "UNSUPPORTED", "CONFLICT", "DATABASE_MISMATCH", "NOT_INITIALIZED", "RESOURCE_EXHAUSTED", "UNAVAILABLE", "CANCELLED", "DEADLINE_EXCEEDED", "SOURCE_FAILED", "OUTCOME_UNKNOWN":
		default:
			return ErrInvalid
		}
	}
	if r.Result != nil {
		v := r.Result
		if !ValidID(v.ID) || !ValidDigest(v.SHA256) || v.Bytes < 0 || v.Bytes > 1<<40 || v.Rows < 0 || v.Rows > 100000000 {
			return ErrInvalid
		}
		switch v.Format {
		case "arrow_ipc", "metadata_v1", "ingestion_state_v1", "ingestion_receipt_v1", "watch_events_v1":
		default:
			return ErrInvalid
		}
	}
	for i, step := range r.Steps {
		if step.Index != i || !ValidDigest(step.SHA256) {
			return ErrInvalid
		}
		switch step.Effect {
		case EffectNone, EffectCommitted, EffectPartial, EffectUnknown:
		default:
			return ErrInvalid
		}
		if r.Effect == EffectNone && step.Effect != EffectNone {
			return ErrInvalid
		}
		if r.Outcome == Completed && step.Effect != EffectNone && step.Effect != EffectCommitted {
			return ErrInvalid
		}
		if r.Outcome == Failed && step.Effect == EffectUnknown {
			return ErrInvalid
		}
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > MaxReceiptBytes {
		return ErrInvalid
	}
	return nil
}

func (r Response) Validate() error {
	if r.Version != Version || !ValidID(r.ID) || !ValidDigest(r.RequestSHA256) {
		return ErrInvalid
	}
	switch r.State {
	case "queued", "assigned", "running":
		if r.Receipt != nil {
			return ErrInvalid
		}
	case string(Completed), string(Rejected), string(Failed), string(CancelledBeforeStart), string(OutcomeUnknown):
		if r.Receipt == nil || r.Receipt.Validate() != nil || r.Receipt.OperationID != r.ID || r.Receipt.RequestSHA256 != r.RequestSHA256 || string(r.Receipt.Outcome) != r.State {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// ValidateBinding pins every response to the original request and handle. An
// empty id is allowed only while accepting the server-generated submit handle.
func (r Response) ValidateBinding(id, digest string) error {
	if r.Validate() != nil || !ValidDigest(digest) || r.RequestSHA256 != digest || (id != "" && r.ID != id) {
		return ErrInvalid
	}
	return nil
}

func (c Capabilities) Validate() error {
	if c.Version != Version || !ValidID(c.Engine) || len(c.Operations) < 1 || len(c.Operations) > 32 {
		return ErrInvalid
	}
	seen := map[Kind]bool{}
	for _, v := range c.Operations {
		if !v.Kind.Valid() || seen[v.Kind] || len(v.ParameterTypes) > 20 || len(v.Transactions) > 2 || len(v.IsolationLevels) > 5 {
			return ErrInvalid
		}
		seen[v.Kind] = true
		types := map[string]bool{}
		for _, typ := range v.ParameterTypes {
			if !parameterType(typ) || types[typ] {
				return ErrInvalid
			}
			types[typ] = true
		}
		txs := map[TransactionMode]bool{}
		for _, tx := range v.Transactions {
			if !validTransaction(tx) || txs[tx] {
				return ErrInvalid
			}
			txs[tx] = true
		}
		levels := map[string]bool{}
		for _, level := range v.IsolationLevels {
			if level == "" || !validIsolation(level) || levels[level] || !txs[TransactionRequired] {
				return ErrInvalid
			}
			levels[level] = true
		}
		switch v.Idempotency {
		case "none":
			if v.IdempotencyRetentionSeconds != 0 {
				return ErrInvalid
			}
		case "transactional", "provider":
			if v.IdempotencyRetentionSeconds < 1 || v.IdempotencyRetentionSeconds > 365*86400 {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
		switch v.Cancellation {
		case "unsupported", "best_effort", "confirmed":
		default:
			return ErrInvalid
		}
	}
	return nil
}

func (c Capabilities) Supports(r Request) error {
	if c.Validate() != nil || r.Validate() != nil {
		return ErrInvalid
	}
	if r.Spec.Native != nil && r.Spec.Native.Provider != c.Engine {
		return ErrUnsupported
	}
	for _, v := range c.Operations {
		if v.Kind != r.Kind {
			continue
		}
		if r.Spec.Statement != nil && r.Spec.Statement.Role != "" && !v.Roles {
			return ErrUnsupported
		}
		var parameters []Parameter
		var tx TransactionMode
		switch {
		case r.Spec.Query != nil:
			parameters = r.Spec.Query.Parameters
		case r.Spec.Statement != nil:
			parameters, tx = r.Spec.Statement.Parameters, r.Spec.Statement.Transaction
			if batch := r.Spec.Statement.Batch; batch != nil {
				for _, statement := range batch.Statements {
					parameters = append(parameters, statement.Parameters...)
				}
			}
			if requested := r.Spec.Statement.Isolation; requested != "" {
				found := false
				for _, level := range v.IsolationLevels {
					found = found || level == requested
				}
				if !found {
					return ErrUnsupported
				}
			}
		case r.Spec.Native != nil:
			parameters = r.Spec.Native.Parameters
		case r.Spec.Schema != nil:
			tx = r.Spec.Schema.Transaction
		case r.Spec.Migration != nil:
			tx = r.Spec.Migration.Transaction
		}
		for _, p := range parameters {
			found := false
			for _, typ := range v.ParameterTypes {
				found = found || p.Type == typ
			}
			if !found {
				return ErrUnsupported
			}
		}
		if tx != "" {
			found := false
			for _, supported := range v.Transactions {
				found = found || tx == supported
			}
			if !found {
				return ErrUnsupported
			}
		}
		return nil
	}
	return ErrUnsupported
}
