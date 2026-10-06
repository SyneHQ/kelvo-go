package operations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// StatementDigest binds one ordered batch entry to its exact SQL and parameter
// representation. Its index is bound separately by StepReceipt and Request.
func StatementDigest(statement BoundStatement) (string, error) {
	if !validSQL(statement.SQL, statement.Parameters) {
		return "", ErrInvalid
	}
	data, err := json.Marshal(statement)
	if err != nil {
		return "", ErrInvalid
	}
	h := sha256.New()
	_, _ = h.Write([]byte("kelvo.operation.statement.v1\x00"))
	_, _ = h.Write(data)
	return hex.EncodeToString(h.Sum(nil)), nil
}
