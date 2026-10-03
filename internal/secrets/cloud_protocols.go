// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func (c *cloudBackend) aws(ctx context.Context, ref Reference, credentials cloudCredentials) (secretValue, error) {
	payload := map[string]string{"SecretId": ref.Secret}
	if ref.Version != "" {
		payload["VersionId"] = ref.Version
	} else {
		payload["VersionStage"] = "AWSCURRENT"
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return secretValue{}, ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/", bytes.NewReader(raw))
	if err != nil {
		return secretValue{}, ErrUnavailable
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "secretsmanager.GetSecretValue")
	digest := sha256.Sum256(raw)
	if err = v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: credentials.AccessKeyID, SecretAccessKey: credentials.SecretAccessKey, SessionToken: credentials.SessionToken}, request, hex.EncodeToString(digest[:]), "secretsmanager", c.config.Region, time.Now()); err != nil {
		return secretValue{}, ErrUnavailable
	}
	var response struct {
		ARN           string
		VersionId     string
		VersionStages []string
		SecretString  *string
		SecretBinary  *string
	}
	if c.decode(request, &response) != nil || response.ARN != ref.Secret || !awsVersion.MatchString(response.VersionId) || (ref.Version != "" && response.VersionId != ref.Version) || ((response.SecretString == nil) == (response.SecretBinary == nil)) {
		return secretValue{}, ErrUnavailable
	}
	if ref.Version == "" {
		current := false
		for _, stage := range response.VersionStages {
			if stage == "AWSCURRENT" {
				current = true
			}
		}
		if !current {
			return secretValue{}, ErrUnavailable
		}
	}
	if response.SecretString != nil {
		return secretValue{data: []byte(*response.SecretString)}, nil
	}
	if base64.StdEncoding.DecodedLen(len(*response.SecretBinary)) > MaxValueBytes+2 {
		return secretValue{}, ErrUnavailable
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(*response.SecretBinary)
	if err != nil {
		wipe(decoded)
		return secretValue{}, ErrUnavailable
	}
	return secretValue{data: decoded}, nil
}

func (c *cloudBackend) azure(ctx context.Context, ref Reference, credentials cloudCredentials) (secretValue, error) {
	path := "/secrets/" + ref.Secret
	if ref.Version != "" {
		path += "/" + ref.Version
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.origin+path+"?api-version=2025-07-01", nil)
	if err != nil {
		return secretValue{}, ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+credentials.Token)
	var response struct {
		Value      *string `json:"value"`
		ID         string  `json:"id"`
		Attributes struct {
			Enabled   *bool  `json:"enabled"`
			Expires   *int64 `json:"exp"`
			NotBefore *int64 `json:"nbf"`
		} `json:"attributes"`
	}
	if c.decode(request, &response) != nil || response.Value == nil || response.Attributes.Enabled == nil || !*response.Attributes.Enabled {
		return secretValue{}, ErrUnavailable
	}
	prefix := c.origin + "/secrets/" + ref.Secret + "/"
	version := strings.TrimPrefix(response.ID, prefix)
	if !strings.HasPrefix(response.ID, prefix) || !azureVersion.MatchString(version) || (ref.Version != "" && version != ref.Version) {
		return secretValue{}, ErrUnavailable
	}
	now := time.Now().Unix()
	var expires time.Time
	if stamp := response.Attributes.Expires; stamp != nil {
		if *stamp <= now || *stamp > 253402300799 {
			return secretValue{}, ErrUnavailable
		}
		expires = time.Unix(*stamp, 0)
	}
	if stamp := response.Attributes.NotBefore; stamp != nil && (*stamp < 0 || *stamp > now) {
		return secretValue{}, ErrUnavailable
	}
	return secretValue{data: []byte(*response.Value), expires: expires}, nil
}

func (c *cloudBackend) gcp(ctx context.Context, ref Reference, credentials cloudCredentials) (secretValue, error) {
	version := ref.Version
	if version == "" {
		version = "latest"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.origin+"/v1/"+ref.Secret+"/versions/"+version+":access", nil)
	if err != nil {
		return secretValue{}, ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+credentials.Token)
	var response struct {
		Name    string `json:"name"`
		Payload struct {
			Data   *string `json:"data"`
			CRC32C *string `json:"dataCrc32c"`
		} `json:"payload"`
	}
	if c.decode(request, &response) != nil || response.Payload.Data == nil || response.Payload.CRC32C == nil {
		return secretValue{}, ErrUnavailable
	}
	prefix := ref.Secret + "/versions/"
	actual := strings.TrimPrefix(response.Name, prefix)
	if !strings.HasPrefix(response.Name, prefix) || !gcpVersion.MatchString(actual) || (ref.Version != "" && actual != ref.Version) {
		return secretValue{}, ErrUnavailable
	}
	expected, err := strconv.ParseUint(*response.Payload.CRC32C, 10, 32)
	if err != nil || base64.StdEncoding.DecodedLen(len(*response.Payload.Data)) > MaxValueBytes+2 {
		return secretValue{}, ErrUnavailable
	}
	data, err := base64.StdEncoding.Strict().DecodeString(*response.Payload.Data)
	if err != nil || crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)) != uint32(expected) {
		wipe(data)
		return secretValue{}, ErrUnavailable
	}
	return secretValue{data: data}, nil
}
