// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func (s *Session) salesforceToken(ctx context.Context) (string, error) {
	if s.username == "" {
		return s.token, nil
	}
	password, securityToken, err := salesforcePassword(s.token)
	if err != nil {
		return "", err
	}
	escape := func(value string) string {
		var b bytes.Buffer
		_ = xml.EscapeText(&b, []byte(value))
		return b.String()
	}
	body := `<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com"><env:Body><urn:login><urn:username>` + escape(s.username) + `</urn:username><urn:password>` + escape(password+securityToken) + `</urn:password></urn:login></env:Body></env:Envelope>`
	raw, err := s.request(ctx, "POST", s.endpoint+"/services/Soap/u/65.0", []byte(body), http.Header{"Content-Type": {"text/xml; charset=UTF-8"}, "SOAPAction": {"login"}}, &budget{limits: adapter.Limits{MaxBytes: 1 << 20}})
	if err != nil {
		return "", err
	}
	var response struct {
		Body struct {
			Login struct {
				Result struct {
					Session string `xml:"sessionId"`
					Server  string `xml:"serverUrl"`
				} `xml:"result"`
			} `xml:"loginResponse"`
		} `xml:"Body"`
	}
	if xml.Unmarshal(raw, &response) != nil || !validBearer(response.Body.Login.Result.Session) {
		return "", errors.New("Salesforce login failed")
	}
	resolved, err := nextAtOrigin(response.Body.Login.Result.Server, s.endpoint, "/services/Soap/")
	if err != nil || resolved == "" {
		return "", errors.New("Salesforce login changed saved instance")
	}
	return response.Body.Login.Result.Session, nil
}
func (s *Session) salesforce(ctx context.Context, query string, limits adapter.Limits) ([]map[string]any, error) {
	if words := strings.Fields(query); len(words) == 0 || !strings.EqualFold(words[0], "SELECT") {
		return nil, adapter.ErrUnsupported
	}
	token, err := s.salesforceToken(ctx)
	if err != nil {
		return nil, err
	}
	endpoint := s.endpoint + "/services/data/v65.0/query?" + url.Values{"q": {query}}.Encode()
	rows := []map[string]any{}
	b := &budget{limits: limits}
	seen := map[string]bool{}
	for page := 0; page < 100; page++ {
		if seen[endpoint] {
			return nil, errors.New("provider repeated page")
		}
		seen[endpoint] = true
		raw, err := s.request(ctx, "GET", endpoint, nil, bearer(token), b)
		if err != nil {
			return nil, err
		}
		object, err := envelope(raw)
		if err != nil {
			return nil, err
		}
		batch, err := objectRows(object["records"])
		if err != nil {
			return nil, err
		}
		if err = b.add(batch); err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		var done bool
		if json.Unmarshal(object["done"], &done) != nil {
			return nil, adapter.ErrInvalid
		}
		if done {
			return rows, nil
		}
		var next string
		if json.Unmarshal(object["nextRecordsUrl"], &next) != nil || next == "" || len(batch) == 0 {
			return nil, adapter.ErrInvalid
		}
		endpoint, err = nextAtOrigin(next, s.endpoint, "/services/data/v65.0/query/")
		if err != nil {
			return nil, err
		}
	}
	return nil, adapter.ErrLimit
}
