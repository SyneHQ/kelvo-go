package watch

import (
	"encoding/json"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

const MaxImportBytes = 16 << 10

// Import carries an operator-approved legacy position, never connection
// credentials. The resolver must verify SourceIdentitySHA256 against its saved
// source binding before releasing fresh credentials; the worker also binds the
// current private revision, scope and generation before installing the cursor.
type Import struct {
	Version              int             `json:"version"`
	ScopeSHA256          string          `json:"scope_sha256"`
	Generation           string          `json:"generation"`
	SourceIdentitySHA256 string          `json:"source_identity_sha256"`
	SourceRevision       string          `json:"source_revision"`
	ResumeToken          json.RawMessage `json:"resume_token"`
}

func (v Import) Validate(scope Scope) error {
	if scope.Validate() != nil || v.Version != Version || v.ScopeSHA256 != scope.Key() || v.Generation != scope.Generation || !operations.ValidDigest(v.SourceIdentitySHA256) || !validText(v.SourceRevision, 256) || len(v.ResumeToken) < 2 || len(v.ResumeToken) > MaxImportBytes-1024 {
		return ErrInvalid
	}
	var token map[string]json.RawMessage
	if provider.DecodeDocument(v.ResumeToken, &token, MaxImportBytes) != nil || len(token) == 0 {
		return ErrInvalid
	}
	return nil
}

// DecodeImport validates sealed bytes before credential resolution. It is not
// evidence of the current source revision; use ParseImport before consumption.
func DecodeImport(raw []byte, scope Scope) (Import, error) {
	// The envelope is a fixed protocol, but BSON resume-token field names are
	// case sensitive. Check exact duplicates recursively, then envelope names
	// explicitly instead of folding keys inside the opaque provider document.
	var fields map[string]json.RawMessage
	if provider.DecodeDocument(raw, &fields, MaxImportBytes) != nil || len(fields) != 6 {
		return Import{}, ErrInvalid
	}
	for _, key := range []string{"version", "scope_sha256", "generation", "source_identity_sha256", "source_revision", "resume_token"} {
		if _, ok := fields[key]; !ok {
			return Import{}, ErrInvalid
		}
	}
	var v Import
	if json.Unmarshal(raw, &v) != nil || v.Validate(scope) != nil {
		return Import{}, ErrInvalid
	}
	return v, nil
}
func ParseImport(raw []byte, scope Scope, revision string) (Import, error) {
	v, err := DecodeImport(raw, scope)
	if err != nil || revision == "" || v.SourceRevision != revision {
		return Import{}, ErrInvalid
	}
	return v, nil
}
func EncodeImport(v Import, scope Scope, revision string) ([]byte, error) {
	if v.Validate(scope) != nil || revision == "" || v.SourceRevision != revision {
		return nil, ErrInvalid
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > MaxImportBytes {
		return nil, ErrLimit
	}
	return raw, nil
}
