package adapter

import (
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/provider"
)

// ValidateSaaSProcessSource validates the private, freshly resolved credential
// envelope. The connector validates provider-specific credential contents.
func ValidateSaaSProcessSource(s ConnectionSpec) error {
	if !provider.SaaS(s.Engine) || s.Engine != strings.ToLower(s.Engine) || s.DSN != "" || s.Password != "" || s.Schema != "" || s.Token == "" || len(s.Token) > 32<<10 || !utf8.ValidString(s.Token) || strings.ContainsRune(s.Token, 0) || len(s.Username) > 4096 || strings.ContainsAny(s.Username, "\x00\r\n") || !provider.ValidSaaSAccount(s.Engine, s.Database) {
		return ErrInvalid
	}
	if s.URL != "" && s.Engine != "salesforce" || s.Username != "" && s.Engine != "google_ads" && s.Engine != "salesforce" || s.Engine == "google_ads" && s.Username == "" || s.Engine == "salesforce" && !provider.ValidSalesforceOrigin(s.URL) {
		return ErrInvalid
	}
	for key, value := range s.Options {
		if s.Engine != "google_ads" || key != "login_customer_id" || !provider.ValidSaaSAccount("google_ads", value) {
			return ErrInvalid
		}
	}
	return nil
}
