// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// MaxListPage bounds one listing response. Callers page with Next and never
// hold more than one page of provider entries in memory.
const MaxListPage = 1000

const listBodyLimit = 4 << 20

// ErrUnsupported means a client cannot list or conditionally delete.
var ErrUnsupported = errors.New("object storage does not support collection")

type Entry struct {
	Key  string
	Size int64
}

// Page is one bounded listing page. Next is empty once the listing is complete.
// A listing is a point-in-time hint: it never proves an object is unreferenced.
type Page struct {
	Entries []Entry
	Next    string
}

// Collector is an opt-in capability used only by remote generation collection.
// Delete removes exactly the supplied version: S3/R2/Azure send If-Match with
// the ETag and GCS sends x-goog-if-generation-match. ErrNotFound means the key
// is already absent; ErrConflict means the version changed and nothing was
// deleted. Every other error may or may not have deleted the object.
type Collector interface {
	List(ctx context.Context, prefix, after string, limit int) (Page, error)
	Delete(ctx context.Context, key, version string) error
}

func validListArgs(location interface{ ValidateKey(string) error }, prefix, after string, limit int) error {
	if limit < 1 || limit > MaxListPage || !strings.HasSuffix(prefix, "/") || location.ValidateKey(prefix+"x") != nil {
		return errors.New("object listing requires a namespaced prefix and bounded page")
	}
	if after != "" && (!strings.HasPrefix(after, prefix) || location.ValidateKey(after) != nil) {
		return errors.New("object listing cursor is outside the prefix")
	}
	return nil
}

func readListBody(body io.Reader, target any) error {
	raw, err := io.ReadAll(io.LimitReader(body, listBodyLimit+1))
	if err != nil {
		return errors.New("object listing could not be read")
	}
	if len(raw) > listBodyLimit {
		return errors.New("object listing exceeds its size limit")
	}
	if err := xml.Unmarshal(raw, target); err != nil {
		return errors.New("object listing is malformed")
	}
	return nil
}

func checkEntries(prefix string, entries []Entry, limit int) error {
	if len(entries) > limit {
		return errors.New("object listing exceeded the requested page")
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Key, prefix) || e.Size < 0 {
			return errors.New("object listing returned an entry outside the prefix")
		}
	}
	return nil
}

type s3ListResult struct {
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
	IsTruncated bool `xml:"IsTruncated"`
}

func (c *s3Client) List(ctx context.Context, prefix, after string, limit int) (Page, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return Page{}, err
	}
	defer op.finish(true)
	if err := validListArgs(c.location, prefix, after, limit); err != nil {
		return Page{}, err
	}
	u := *c.origin
	u.Path = "/" + c.location.Bucket
	q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {strconv.Itoa(limit)}}
	if after != "" {
		q.Set("start-after", after)
	}
	u.RawQuery = q.Encode()
	r, err := http.NewRequestWithContext(op.ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Page{}, errors.New("object request could not be prepared")
	}
	resp, err := c.do(op, r, "")
	if err != nil {
		return Page{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Page{}, errors.New("object storage rejected listing")
	}
	var result s3ListResult
	if err := readListBody(resp.Body, &result); err != nil {
		return Page{}, err
	}
	page := Page{}
	for _, item := range result.Contents {
		page.Entries = append(page.Entries, Entry{Key: item.Key, Size: item.Size})
	}
	if err := checkEntries(prefix, page.Entries, limit); err != nil {
		return Page{}, err
	}
	if result.IsTruncated {
		if len(page.Entries) == 0 {
			return Page{}, errors.New("object listing made no progress")
		}
		page.Next = page.Entries[len(page.Entries)-1].Key
	}
	return page, nil
}

// Some S3-compatible providers (MinIO observed) ignore If-Match on DELETE, so
// a conditional HEAD runs first. Generation keys are never rewritten, which
// closes the remaining HEAD-to-DELETE window in practice; it is not atomic.
func (c *s3Client) Delete(ctx context.Context, key, version string) error {
	if version != "" {
		if _, err := c.Head(ctx, key, version); err != nil {
			return err
		}
	}
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return err
	}
	defer op.finish(true)
	if version == "" {
		return errors.New("object deletion requires an exact version")
	}
	r, err := c.request(op.ctx, http.MethodDelete, key, nil, 0, "", Condition{})
	if err != nil {
		return err
	}
	if err := c.readCondition(r, version); err != nil {
		return err
	}
	resp, err := c.do(op, r, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK:
		return nil
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusPreconditionFailed, http.StatusConflict:
		return ErrConflict
	}
	return errors.New("object storage rejected deletion")
}

type azureListResult struct {
	Blobs struct {
		Blob []struct {
			Name       string `xml:"Name"`
			Properties struct {
				ContentLength int64 `xml:"Content-Length"`
			} `xml:"Properties"`
		} `xml:"Blob"`
	} `xml:"Blobs"`
	NextMarker string `xml:"NextMarker"`
}

// Azure pages with an opaque marker. Next carries that marker; callers treat
// it as opaque and pass it back as after.
func (c *azureClient) List(ctx context.Context, prefix, after string, limit int) (Page, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return Page{}, err
	}
	defer op.finish(true)
	if err := validListArgs(c.location, prefix, "", limit); err != nil || len(after) > 1024 || strings.ContainsAny(after, "\r\n\x00") {
		return Page{}, errors.New("object listing requires a namespaced prefix and bounded page")
	}
	u := *c.origin
	u.Path = "/" + c.location.Bucket
	q := url.Values{}
	for k, v := range c.sas {
		q[k] = v
	}
	q.Set("restype", "container")
	q.Set("comp", "list")
	q.Set("prefix", prefix)
	q.Set("maxresults", strconv.Itoa(limit))
	if after != "" {
		q.Set("marker", after)
	}
	u.RawQuery = q.Encode()
	r, err := http.NewRequestWithContext(op.ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Page{}, errors.New("Azure snapshot request could not be prepared")
	}
	r.Header.Set("x-ms-version", azureStorageVersion)
	resp, err := c.do(op, r, http.StatusOK)
	if err != nil {
		return Page{}, err
	}
	defer resp.Body.Close()
	var result azureListResult
	if err := readListBody(resp.Body, &result); err != nil {
		return Page{}, err
	}
	page := Page{Next: result.NextMarker}
	for _, blob := range result.Blobs.Blob {
		page.Entries = append(page.Entries, Entry{Key: blob.Name, Size: blob.Properties.ContentLength})
	}
	return page, checkEntries(prefix, page.Entries, limit)
}

func (c *azureClient) Delete(ctx context.Context, key, version string) error {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return err
	}
	defer op.finish(true)
	if version == "" {
		return errors.New("object deletion requires an exact version")
	}
	r, err := c.request(op.ctx, http.MethodDelete, key, version, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(op, r, http.StatusAccepted)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}
