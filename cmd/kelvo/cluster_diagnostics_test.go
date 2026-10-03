// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"errors"
	"io"
	"os"
	"testing"
)

func TestStoreMetadataDiagnosticCLIDoesNotLogUntypedErrors(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	original := os.Stderr
	os.Stderr = writer
	defer func() { os.Stderr = original }()
	for _, err := range []error{
		nil,
		errors.New("cluster metadata unavailable"),
		errors.New("KELVO_CLUSTER_METADATA {\"phase\":\"private-token\"}"),
		errors.New("private-password tls://private-host:4222 metadata failure"),
	} {
		reportStoreMetadataDiagnostic(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stderr = original
	output, err := io.ReadAll(reader)
	if err != nil || len(output) != 0 {
		t.Fatal("untyped error reached the structured diagnostic output")
	}
}
