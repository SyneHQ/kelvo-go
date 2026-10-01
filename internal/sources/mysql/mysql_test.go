// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mysql

import (
	"crypto/tls"
	"testing"
	"time"
)

const safeDSN = "u:p@tcp(db.example:3306)/analytics?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27"

func TestTLSAndTimeDSN(t *testing.T) {
	config, err := parseConfig(safeDSN)
	if err != nil {
		t.Fatal(err)
	}
	if config.TLS == nil || config.TLS.MinVersion != tls.VersionTLS12 || config.TLS.InsecureSkipVerify || config.Loc != time.UTC || !config.ParseTime || config.Params["time_zone"] != "'+00:00'" {
		t.Fatal("TLS/time configuration changed")
	}
	for _, dsn := range []string{
		"u:p@unix(/tmp/mysql.sock)/analytics?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27", "u:p@tcp(db:3306)/analytics?tls=true", "u:p@tcp(db:3306)/analytics?tls=skip-verify&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27",
		"u:p@/analytics?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27", "u:@tcp(db:3306)/analytics?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27",
		safeDSN + "&tls=skip-verify", safeDSN + "&parseTime=false", safeDSN + "&loc=Local", safeDSN + "&time_zone=%27%2B05%3A30%27", safeDSN + "&multiStatements=true", safeDSN + "&allowAllFiles=true", safeDSN + "&allowFallbackToPlaintext=true", safeDSN + "&allowCleartextPasswords=true", safeDSN + "&allowOldPasswords=true", safeDSN + "&interpolateParams=true", safeDSN + "&serverPubKey=file", safeDSN + "&sql_mode=NO_BACKSLASH_ESCAPES", safeDSN + "&t%6Cs=true", safeDSN + "&timeout=0", safeDSN + "&timeout=2h", safeDSN + "&unknown", safeDSN + "&charset=latin1",
	} {
		if validateDSN(dsn) == nil {
			t.Fatal("unsafe DSN accepted")
		}
	}
}
