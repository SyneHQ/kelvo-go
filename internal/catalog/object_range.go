// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// ObjectRange is a query-lived capability served by the parent on loopback.
// It contains neither cloud credentials nor an upstream object URL.
type ObjectRange struct {
	URL   string `json:"url"`
	Bytes int64  `json:"bytes"`
}

var rangePath = regexp.MustCompile(`^/[0-9a-f]{64}/[A-Za-z_][A-Za-z0-9_]{0,62}(?:/part-[0-9]{4})?$`)

func (r ObjectRange) Validate() error {
	u, err := url.Parse(r.URL)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || !rangePath.MatchString(u.Path) || r.Bytes <= 0 || r.Bytes > 4<<30 {
		return errors.New("object range source must be an exact query capability on loopback")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return errors.New("object range source requires a valid loopback port")
	}
	return nil
}

// ValidateObjectRanges rejects mixed local/cloud metadata and scopes every leaf
// to one ordered dataset capability minted by the trusted parent.
func (s Source) ValidateObjectRanges() error {
	if s.Ranges == nil {
		return nil
	}
	if s.Type != "parquet" || len(s.Ranges) == 0 || len(s.Ranges) > 256 || s.Range != nil || s.Path != "" || s.ParquetPaths != nil || s.Object != nil || s.Adapter != "" || s.Federation != nil || s.DSNEnv != "" || s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "" || len(s.Options) != 0 || !ValidID(s.ID) {
		return errors.New("invalid multipart object range source")
	}
	var prefix string
	for i, r := range s.Ranges {
		if err := r.Validate(); err != nil {
			return err
		}
		u, _ := url.Parse(r.URL)
		components := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
		if len(components) != 3 || components[1] != s.ID || components[2] != fmt.Sprintf("part-%04d", i) {
			return errors.New("multipart capability must identify its dataset and sequential part")
		}
		current := u.Scheme + "://" + u.Host + "/" + components[0] + "/" + components[1]
		if i > 0 && current != prefix {
			return errors.New("multipart capabilities must share one authority and scope")
		}
		prefix = current
	}
	return nil
}

// ValidateRangeSelection bounds a query's capabilities, including legacy leaves.
func ValidateRangeSelection(sources []Source) error {
	count := 0
	for _, s := range sources {
		if err := s.ValidateObjectRanges(); err != nil {
			return err
		}
		count += len(s.Ranges)
		if s.Range != nil {
			count++
		}
		if count > 1024 {
			return errors.New("query object range capability limit exceeded")
		}
	}
	return nil
}
