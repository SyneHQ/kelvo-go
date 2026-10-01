//go:build !linux && !darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import "os"

func storePlatformSupported() error                     { return ErrUnsupportedPlatform }
func storeOpenDirectory(string, bool) (*os.File, error) { return nil, ErrUnsupportedPlatform }
func storeOpenChildDirectory(*os.File, string, bool) (*os.File, error) {
	return nil, ErrUnsupportedPlatform
}
func storeOpenFile(*os.File, string, int, os.FileMode) (*os.File, error) {
	return nil, ErrUnsupportedPlatform
}
func storeCheckPrivate(*os.File, bool, bool) error       { return ErrUnsupportedPlatform }
func storeTryLock(*os.File, bool) (bool, error)          { return false, ErrUnsupportedPlatform }
func storeRename(*os.File, string, string) error         { return ErrUnsupportedPlatform }
func storePublishPayload(*os.File, string, string) error { return ErrUnsupportedPlatform }
func storeRemove(*os.File, string) error                 { return ErrUnsupportedPlatform }
