// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"regexp"
	"strings"
)

var lambdaFunctionName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func validateLambdaProcessSource(s ConnectionSpec) error {
	region := s.Options["region"]
	if !nativeReaderRegion.MatchString(region) || len(region) > 64 || !lambdaFunctionName.MatchString(s.Options["function_name"]) || s.Database == "" || s.Schema != "" || s.Username == "" || s.Password == "" || len(s.Username) > 1024 || len(s.Password) > 4096 {
		return ErrInvalid
	}
	suffix := "amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		suffix += ".cn"
	}
	if s.URL != "https://lambda."+region+"."+suffix {
		return ErrInvalid
	}
	path := s.Options["bucket_path"]
	if !strings.HasPrefix(path, "/") || len(path) > 4096 || strings.ContainsAny(path, "\\\x00\r\n") {
		return ErrInvalid
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." || part == "." {
			return ErrInvalid
		}
	}
	return nil
}
