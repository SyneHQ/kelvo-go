// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import "testing"

func TestLambdaPinsRegionalInvocationAndSavedPath(t *testing.T) {
	for _, change := range []func(*ConnectionSpec){
		func(s *ConnectionSpec) { s.URL = "https://attacker.example" },
		func(s *ConnectionSpec) { s.URL = "https://lambda.us-west-2.amazonaws.com" },
		func(s *ConnectionSpec) { s.Options["function_name"] = "fn:alias" },
		func(s *ConnectionSpec) { s.Options["bucket_path"] = "/safe/../other" },
		func(s *ConnectionSpec) { s.Options["bucket_path"] = "relative" },
		func(s *ConnectionSpec) { s.Schema = "unbound" },
		func(s *ConnectionSpec) { s.Username = "" },
		func(s *ConnectionSpec) { s.Password = "" },
	} {
		s := nativeReaderFixture("clickhouse_lambda")
		change(&s)
		if ValidateNativeReaderProcessSource(s) == nil {
			t.Fatal("unsafe Lambda source accepted")
		}
	}
	s := nativeReaderFixture("clickhouse_lambda")
	s.Options["region"] = "cn-north-1"
	s.URL = "https://lambda.cn-north-1.amazonaws.com.cn"
	if err := ValidateNativeReaderProcessSource(s); err != nil {
		t.Fatal("valid regional source rejected", err)
	}
}
