// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sheets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const driveScope = "https://www.googleapis.com/auth/drive.metadata.readonly"

// sheetCache is the on-disk snapshot cache shared by every adapter process.
type sheetCache struct {
	dir      string
	maxBytes int64
}

type cacheMeta struct {
	SpreadsheetID string    `json:"spreadsheet_id"`
	Version       string    `json:"version"`
	BuiltAt       time.Time `json:"built_at"`
	Bytes         int64     `json:"bytes"`
	SHA256        string    `json:"sha256"`
}

// cacheFromEnv returns nil unless both variables are valid: no cache, today's behaviour.
func cacheFromEnv() *sheetCache {
	dir := os.Getenv("KELVO_SHEETS_CACHE_DIR")
	mb, err := strconv.ParseInt(os.Getenv("KELVO_SHEETS_CACHE_MAX_MB"), 10, 64)
	if err != nil || mb < 1 || mb > 1<<30 || !filepath.IsAbs(dir) {
		return nil
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() || syscall.Access(dir, 0x2|0x1) != nil {
		return nil
	}
	return &sheetCache{dir: filepath.Clean(dir), maxBytes: mb << 20}
}

// serviceAccount reports whether the secret can mint a Drive token of its own;
// a raw access token is scoped to Sheets only and never uses the cache.
func serviceAccount(credential string) bool {
	var secret struct {
		Type string `json:"type"`
	}
	return json.Unmarshal([]byte(credential), &secret) == nil && secret.Type == "service_account"
}

// driveVersion is the freshness check and the proof the credential still reads the sheet.
func (s *Session) driveVersion(ctx context.Context) (string, bool) {
	if s.http == nil || !serviceAccount(s.credential) {
		return "", false
	}
	token, err := s.mint(ctx, s.credential, driveScope)
	if err != nil {
		return "", false
	}
	var file struct {
		Version      string `json:"version"`
		ModifiedTime string `json:"modifiedTime"`
	}
	remaining := int64(64 << 10)
	if s.get(ctx, s.driveOrigin+"/drive/v3/files/"+s.id+"?fields=version,modifiedTime&supportsAllDrives=true", token, &remaining, &file) != nil {
		return "", false
	}
	if _, err := strconv.ParseUint(file.Version, 10, 63); err != nil {
		return "", false
	}
	return file.Version, true
}

func (s *Session) cacheKey(version string) string {
	caps := strings.Join([]string{strconv.Itoa(maxSheetTabs), strconv.Itoa(maxSheetRows), strconv.Itoa(maxSheetColumns), strconv.FormatInt(int64(s.limits.MemoryMB)<<17, 10)}, ",")
	sum := sha256.Sum256([]byte("kelvo.sheets.cache.v1\x00" + s.id + "\x00" + version + "\x00" + caps))
	return hex.EncodeToString(sum[:])
}

// lock opens $DIR/<key>.lock and takes how (LOCK_EX or LOCK_SH, optionally
// |LOCK_NB), polling so ctx bounds the wait. remove() unlinks lock files, so a
// lock taken on an unlinked inode is dropped and retried.
func (c *sheetCache) lock(ctx context.Context, key string, how int) (*os.File, error) {
	path := filepath.Join(c.dir, key+".lock")
	for range 5 {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		if err = flockWait(ctx, file, how); err != nil {
			file.Close()
			return nil, err
		}
		if current(file, path) {
			return file, nil
		}
		file.Close()
	}
	return nil, syscall.EAGAIN
}

// relock converts a held lock; conversion is not atomic, so it re-checks the inode.
func (c *sheetCache) relock(ctx context.Context, file *os.File, key string, how int) error {
	// Drop the held lock first: waiting to convert a shared lock while other
	// processes do the same deadlocks where a failed conversion keeps the old
	// lock (BSD). Callers re-check the entry after the new lock is granted.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return err
	}
	if err := flockWait(ctx, file, how); err != nil {
		return err
	}
	if !current(file, filepath.Join(c.dir, key+".lock")) {
		return syscall.EAGAIN
	}
	return nil
}

func current(file *os.File, path string) bool {
	held, err := file.Stat()
	named, err2 := os.Stat(path)
	return err == nil && err2 == nil && os.SameFile(held, named)
}

// flockWait polls a non-blocking flock with backoff until ctx is done.
// With LOCK_NB in how it tries once.
func flockWait(ctx context.Context, file *os.File, how int) error {
	wait := 50 * time.Millisecond
	for {
		err := syscall.Flock(int(file.Fd()), how|syscall.LOCK_NB)
		if err == nil || how&syscall.LOCK_NB != 0 || err != syscall.EWOULDBLOCK {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, 500*time.Millisecond)
	}
}

func (c *sheetCache) meta(key string) (cacheMeta, bool) {
	var meta cacheMeta
	raw, err := os.ReadFile(filepath.Join(c.dir, key, "meta.json"))
	if err != nil || json.Unmarshal(raw, &meta) != nil || meta.Bytes < 1 || len(meta.SHA256) != 64 {
		return meta, false
	}
	return meta, true
}

// publish moves a fully written, fsynced build directory to $DIR/<key>. The
// caller holds the key's exclusive lock, so replacing an old entry is safe.
func (c *sheetCache) publish(key, tmp string, meta cacheMeta) error {
	raw, _ := json.Marshal(meta)
	if err := writeSynced(filepath.Join(tmp, "meta.json"), raw); err != nil {
		return err
	}
	if err := syncPath(tmp); err != nil {
		return err
	}
	entry := filepath.Join(c.dir, key)
	if _, err := os.Stat(entry); err == nil {
		old := filepath.Join(c.dir, ".tmp-old-"+key+"-"+strconv.FormatInt(time.Now().UnixNano(), 36))
		if err = os.Rename(entry, old); err != nil {
			return err
		}
		defer os.RemoveAll(old)
	}
	if err := os.Rename(tmp, entry); err != nil {
		return err
	}
	return syncPath(c.dir)
}

// evict drops older versions of this spreadsheet, then least-recently-used
// entries until the cache fits. It never touches keep or an entry whose lock is held.
func (c *sheetCache) evict(keep, spreadsheetID, version string) {
	names, _ := os.ReadDir(c.dir)
	type entry struct {
		key  string
		size int64
		used time.Time
	}
	var entries []entry
	var total int64
	for _, name := range names {
		key := name.Name()
		if name.IsDir() && strings.HasPrefix(key, ".tmp-") { // builds in flight, or left by a crash
			if info, err := name.Info(); err == nil && time.Since(info.ModTime()) > 30*time.Minute {
				os.RemoveAll(filepath.Join(c.dir, key))
			} else {
				total += dirSize(filepath.Join(c.dir, key))
			}
			continue
		}
		if !name.IsDir() || len(key) != 64 || strings.Trim(key, "0123456789abcdef") != "" {
			continue
		}
		meta, ok := c.meta(key)
		info, err := os.Stat(filepath.Join(c.dir, key, "meta.json"))
		if key != keep && (!ok || err != nil || meta.SpreadsheetID == spreadsheetID && meta.Version != version) {
			c.remove(key) // an older version of this sheet, or a broken entry
			continue
		}
		if err != nil {
			continue
		}
		entries = append(entries, entry{key, meta.Bytes, info.ModTime()})
		total += meta.Bytes
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].used.Before(entries[j].used) })
	for _, e := range entries {
		if total <= c.maxBytes {
			return
		}
		if e.key != keep && c.remove(e.key) {
			total -= e.size
		}
	}
}

// remove deletes an entry only when nobody is building or reading it.
func (c *sheetCache) remove(key string) bool {
	lock, err := c.lock(context.Background(), key, syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return false
	}
	defer lock.Close()
	if os.RemoveAll(filepath.Join(c.dir, key)) != nil {
		return false
	}
	os.Remove(filepath.Join(c.dir, key+".lock")) // lock() re-checks the inode, so waiters retry
	return true
}

func dirSize(dir string) (total int64) {
	filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

func writeSynced(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func syncPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
