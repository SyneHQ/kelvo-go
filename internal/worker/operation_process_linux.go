//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/SYNEHQ/kelvo-go/operations"
	"golang.org/x/sys/unix"
)

type operationBinaryIdentity struct{ info os.FileInfo }

func openOperationBinary(ctx context.Context, cfg OperationProcessConfig) (*os.File, operationBinaryIdentity, error) {
	invalid := func() (*os.File, operationBinaryIdentity, error) {
		return nil, operationBinaryIdentity{}, operationFailure("CONFIGURATION_ERROR")
	}
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(cfg.Binary) || filepath.Clean(cfg.Binary) != cfg.Binary || len(cfg.Binary) > 4096 || !operations.ValidDigest(cfg.SHA256) {
		return invalid()
	}
	resolved, err := filepath.EvalSymlinks(cfg.Binary)
	if err != nil || resolved != cfg.Binary {
		return invalid()
	}
	fd, err := unix.Open(cfg.Binary, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return invalid()
	}
	file := os.NewFile(uintptr(fd), cfg.Binary)
	keep := false
	defer func() {
		if !keep {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 || info.Mode()&0022 != 0 || info.Size() < 1 || info.Size() > maximumOperationBinaryBytes {
		return invalid()
	}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	var read int64
	for {
		if ctx.Err() != nil {
			return invalid()
		}
		n, readErr := file.Read(buffer)
		if n > 0 {
			read += int64(n)
			if read > info.Size() {
				return invalid()
			}
			_, _ = hash.Write(buffer[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return invalid()
		}
	}
	identity := operationBinaryIdentity{info: info}
	if read != info.Size() || hex.EncodeToString(hash.Sum(nil)) != cfg.SHA256 || operationBinaryCurrent(file, identity) != nil || ctx.Err() != nil {
		return invalid()
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return invalid()
	}
	keep = true
	return file, identity, nil
}

func operationBinaryCurrent(file *os.File, identity operationBinaryIdentity) error {
	if file == nil || identity.info == nil {
		return operationFailure("CONFIGURATION_ERROR")
	}
	current, err := file.Stat()
	if err != nil || !os.SameFile(current, identity.info) || current.Size() != identity.info.Size() || current.Mode() != identity.info.Mode() || !current.ModTime().Equal(identity.info.ModTime()) {
		return operationFailure("CONFIGURATION_ERROR")
	}
	return nil
}
