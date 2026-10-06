package filesnapshot

import "github.com/SYNEHQ/kelvo-go/operations"

// Publication binds an immutable candidate to one already dispatched operation.
// It contains no path, credential, SQL body or file bytes.
type Publication struct {
	Version       int        `json:"version"`
	OperationID   string     `json:"operation_id"`
	RequestSHA256 string     `json:"request_sha256"`
	Original      Descriptor `json:"original"`
	Replacement   Descriptor `json:"replacement"`
}

func (p Publication) Validate() error {
	if p.Version != 1 || !operations.ValidID(p.OperationID) || !operations.ValidDigest(p.RequestSHA256) || p.Original.Validate() != nil || p.Replacement.Validate() != nil || p.Original.Format != p.Replacement.Format || (p.Original.Format != "sqlite" && p.Original.Format != "duckdb") {
		return ErrInvalid
	}
	return nil
}

type PublicationReceipt struct {
	Version           int    `json:"version"`
	OperationID       string `json:"operation_id"`
	RequestSHA256     string `json:"request_sha256"`
	ReplacementSHA256 string `json:"replacement_sha256"`
	Committed         bool   `json:"committed"`
}

func (r PublicationReceipt) Matches(p Publication) bool {
	return p.Validate() == nil && r.Version == 1 && r.OperationID == p.OperationID && r.RequestSHA256 == p.RequestSHA256 && r.ReplacementSHA256 == p.Replacement.SHA256
}
