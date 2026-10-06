package adapter

import (
	"github.com/SYNEHQ/kelvo-go/provider"
	"strings"
	"unicode/utf8"
)

func ValidateSheetsProcessSource(s ConnectionSpec) error {
	if s.Engine != "google_sheets" || s.DSN != "" || s.URL != "" || s.Username != "" || s.Password != "" || s.Schema != "" || s.Token == "" || len(s.Token) > 32<<10 || !utf8.ValidString(s.Token) || strings.ContainsRune(s.Token, 0) || len(s.Options) != 1 || !provider.ValidSheetID(s.Options["spreadsheet_id"]) {
		return ErrInvalid
	}
	return nil
}
