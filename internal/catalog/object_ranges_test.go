package catalog

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func rangeListFixture(id string, n int) Source {
	s := Source{ID: id, Type: "parquet"}
	for i := 0; i < n; i++ {
		s.Ranges = append(s.Ranges, ObjectRange{URL: fmt.Sprintf("http://127.0.0.1:12345/%s/%s/part-%04d", strings.Repeat("a", 64), id, i), Bytes: 100})
	}
	return s
}
func TestMultipartObjectCapabilities(t *testing.T) {
	for name, change := range map[string]func(*Source){"valid": func(*Source) {}, "path": func(s *Source) { s.Path = "/tmp/x" }, "range": func(s *Source) { s.Range = &s.Ranges[0] }, "parquet": func(s *Source) { s.ParquetPaths = []string{"/tmp/x"} }, "secret": func(s *Source) { s.TokenEnv = "KELVO_SOURCE_SECRET" }, "csv": func(s *Source) { s.Type = "csv" }, "duplicate": func(s *Source) { s.Ranges[1] = s.Ranges[0] }, "authority": func(s *Source) { s.Ranges[1].URL = strings.Replace(s.Ranges[1].URL, ":12345", ":12346", 1) }, "token": func(s *Source) {
		s.Ranges[1].URL = strings.Replace(s.Ranges[1].URL, strings.Repeat("a", 64), strings.Repeat("b", 64), 1)
	}, "dataset": func(s *Source) { s.ID = "other" }, "query": func(s *Source) { s.Ranges[0].URL += "?secret=x" }, "empty": func(s *Source) { s.Ranges = []ObjectRange{} }, "too-many": func(s *Source) { *s = rangeListFixture("data", 257) }} {
		t.Run(name, func(t *testing.T) {
			s := rangeListFixture("data", 2)
			change(&s)
			if (s.ValidateObjectRanges() == nil) != (name == "valid") {
				t.Fatal("unexpected capability validation")
			}
		})
	}
	s := rangeListFixture("data", 2)
	b, _ := json.Marshal(s)
	var out Source
	if json.Unmarshal(b, &out) != nil || len(out.Ranges) != 2 || out.ValidateObjectRanges() != nil {
		t.Fatal("IPC roundtrip failed")
	}
	content := strings.Replace(acceleratedYAML, "    type: clickhouse", "    object_ranges: []\n    type: clickhouse", 1)
	if _, err := loadAccelerationFixture(t, content); err == nil {
		t.Fatal("YAML accepted internal ranges")
	}
	var sources []Source
	for i := 0; i < 4; i++ {
		sources = append(sources, rangeListFixture(fmt.Sprintf("data%d", i), 256))
	}
	if ValidateRangeSelection(sources) != nil {
		t.Fatal("1024 rejected")
	}
	sources = append(sources, rangeListFixture("extra", 1))
	if ValidateRangeSelection(sources) == nil {
		t.Fatal("1025 accepted")
	}
}
