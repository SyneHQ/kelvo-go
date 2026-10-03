//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateDocumentPreservesSourceSecretLimitAndFileSafety(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "keys.yml")
	payload := bytes.Repeat([]byte("x"), MaxValueBytes+1)
	if err := os.WriteFile(file, payload, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPrivateDocument(context.Background(), file, MaxDocumentBytes)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("bounded document unavailable", err)
	}
	if _, err := readPrivateFile(context.Background(), file); !errors.Is(err, ErrUnavailable) {
		t.Fatal("source secret limit increased", err)
	}
	if _, err := ReadPrivateDocument(context.Background(), file, len(payload)-1); !errors.Is(err, ErrUnavailable) {
		t.Fatal("document exceeded caller limit", err)
	}
	for _, path := range []string{"relative", root + "/../keys", "/"} {
		if _, err := ReadPrivateDocument(context.Background(), path, 1); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid path accepted")
		}
	}
	for _, limit := range []int{0, MaxDocumentBytes + 1} {
		if _, err := ReadPrivateDocument(context.Background(), file, limit); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid bound accepted")
		}
	}
	if err := os.Chmod(file, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateDocument(context.Background(), file, MaxDocumentBytes); !errors.Is(err, ErrUnavailable) {
		t.Fatal("public file accepted")
	}
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateDocument(context.Background(), link, MaxDocumentBytes); !errors.Is(err, ErrUnavailable) {
		t.Fatal("symlink accepted")
	}
	if err := os.Link(file, filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateDocument(context.Background(), file, MaxDocumentBytes); !errors.Is(err, ErrUnavailable) {
		t.Fatal("hardlink accepted")
	}
}
