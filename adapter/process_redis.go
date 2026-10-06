package adapter

import (
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

func ValidateRedisProcessSource(s ConnectionSpec) error {
	u, err := url.Parse(s.URL)
	database, dbErr := strconv.Atoi(s.Database)
	if s.Engine != "redis" || s.DSN != "" || s.Token != "" || s.Schema != "" || dbErr != nil || database < 0 || database > 255 || strconv.Itoa(database) != s.Database || err != nil || u.Scheme != "rediss" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(s.URL, "\\\x00\r\n\t ") {
		return ErrInvalid
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || s.Password == "" {
		return ErrInvalid
	}
	for _, value := range []string{s.Username, s.Password} {
		if len(value) > 32<<10 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return ErrInvalid
		}
	}
	for key, value := range s.Options {
		if key != "tls_ca_pem" && key != "tls_server_name" || len(value) > 64<<10 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return ErrInvalid
		}
	}
	return nil
}
