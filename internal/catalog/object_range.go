// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"net/url"
	"regexp"
	"strconv"
)

// ObjectRange is a query-lived capability served by the parent on loopback.
// It contains neither cloud credentials nor an upstream object URL.
type ObjectRange struct {
	URL   string `json:"url"`
	Bytes int64  `json:"bytes"`
}

var rangePath = regexp.MustCompile(`^/[0-9a-f]{64}/[A-Za-z_][A-Za-z0-9_]{0,62}$`)

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
