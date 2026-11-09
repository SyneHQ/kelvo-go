package sheets

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestCacheFromEnvRequiresBothValidVariables(t *testing.T) {
	dir := t.TempDir()
	for _, env := range [][2]string{{"", ""}, {dir, ""}, {"", "10"}, {"relative", "10"}, {dir, "0"}, {dir, "x"}, {filepath.Join(dir, "missing"), "10"}} {
		t.Setenv("KELVO_SHEETS_CACHE_DIR", env[0])
		t.Setenv("KELVO_SHEETS_CACHE_MAX_MB", env[1])
		if cacheFromEnv() != nil {
			t.Fatal("cache enabled for", env)
		}
	}
	t.Setenv("KELVO_SHEETS_CACHE_DIR", dir)
	t.Setenv("KELVO_SHEETS_CACHE_MAX_MB", "10")
	if c := cacheFromEnv(); c == nil || c.maxBytes != 10<<20 {
		t.Fatal(c)
	}
}

func TestCacheKeyCoversSheetVersionAndBudget(t *testing.T) {
	a := &Session{id: "saved-sheet", limits: testLimits()}
	b := &Session{id: "other-sheet", limits: testLimits()}
	c := &Session{id: "saved-sheet", limits: testLimits()}
	c.limits.MemoryMB *= 2
	if a.cacheKey("1") == a.cacheKey("2") || a.cacheKey("1") == b.cacheKey("1") || a.cacheKey("1") == c.cacheKey("1") || len(a.cacheKey("1")) != 64 {
		t.Fatal("cache key collision")
	}
}

func TestEvictKeepsBudgetByLeastRecentUse(t *testing.T) {
	c := &sheetCache{dir: t.TempDir(), maxBytes: 1 << 20}
	s := &Session{limits: testLimits()}
	keys := make([]string, 4)
	for i := range keys {
		s.id = "sheet-" + strconv.Itoa(i)
		keys[i] = s.cacheKey("1")
		dir := filepath.Join(c.dir, keys[i])
		os.Mkdir(dir, 0o700)
		raw, _ := json.Marshal(cacheMeta{SpreadsheetID: s.id, Version: "1", Bytes: 400 << 10, SHA256: keys[i]})
		os.WriteFile(filepath.Join(dir, "meta.json"), raw, 0o600)
		used := time.Now().Add(time.Duration(i-10) * time.Minute)
		os.Chtimes(filepath.Join(dir, "meta.json"), used, used)
	}
	held, err := c.lock(context.Background(), keys[1], 1) // LOCK_SH: a reader is using the second-oldest entry
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	c.evict(keys[0], "sheet-0", "1") // keys[0] just published, though oldest
	exists := func(key string) bool { _, err := os.Stat(filepath.Join(c.dir, key)); return err == nil }
	if !exists(keys[0]) || !exists(keys[1]) || exists(keys[2]) || exists(keys[3]) {
		t.Fatal(exists(keys[0]), exists(keys[1]), exists(keys[2]), exists(keys[3]))
	}
}

func TestEvictSweepsStaleBuildsAndRemoveUnlinksLock(t *testing.T) {
	c := &sheetCache{dir: t.TempDir(), maxBytes: 1 << 30}
	stale, live := filepath.Join(c.dir, ".tmp-old-x"), filepath.Join(c.dir, ".tmp-y")
	os.Mkdir(stale, 0o700)
	os.Mkdir(live, 0o700)
	old := time.Now().Add(-time.Hour)
	os.Chtimes(stale, old, old)
	key := (&Session{id: "s", limits: testLimits()}).cacheKey("1")
	os.Mkdir(filepath.Join(c.dir, key), 0o700)
	lock, _ := c.lock(context.Background(), key, 2) // LOCK_EX, then released
	lock.Close()
	c.evict("", "other", "1")
	for path, want := range map[string]bool{stale: false, live: true, filepath.Join(c.dir, key): false, filepath.Join(c.dir, key+".lock"): false} {
		if _, err := os.Stat(path); (err == nil) != want {
			t.Fatal(path, want)
		}
	}
}
