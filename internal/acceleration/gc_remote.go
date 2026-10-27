// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

// Remote generation collection. Fail closed: any doubt about a manifest,
// writer, registry document, reader lease, listing or provider outcome keeps
// every object. Deletion is opt-in and needs a protected namespace.

const (
	MinRemoteGCGrace       = 10 * time.Minute
	DefaultRemoteGCGrace   = 24 * time.Hour
	remoteGCMaxGenerations = 4096
	remoteGCMaxObjects     = 1 << 20
	remoteGCReportLimit    = 256
	remoteGCWriterMargin   = time.Minute
	remoteGCMarkerLimit    = 4 << 10
)

var (
	gcDataName     = regexp.MustCompile(`^([0-9a-f]{32})(\.parquet|\.parts\.yaml|-part-[0-9]{4}\.parquet)$`)
	gcRegistryName = regexp.MustCompile(`^reader-leases/([0-9a-f]{32})\.yml$`)
	gcMarkerName   = regexp.MustCompile(`^gc/([0-9a-f]{32})\.quarantine\.yaml$`)
)

// RemoteGCOptions defaults to a dry run. Delete enables quarantine and, after
// Grace has passed on the provider clock, exact-version deletion.
type RemoteGCOptions struct {
	Delete bool
	Grace  time.Duration
}

type RemoteGCBytes struct {
	Objects int64 `yaml:"objects"`
	Bytes   int64 `yaml:"bytes"`
}

type RemoteGCGeneration struct {
	Generation string `yaml:"generation"`
	Class      string `yaml:"class"`
	Objects    int    `yaml:"objects"`
	Bytes      int64  `yaml:"bytes"`
	Action     string `yaml:"action"`
}

// RemoteGCReport never contains keys outside the dataset, URLs or credentials.
type RemoteGCReport struct {
	Dataset     string                   `yaml:"dataset"`
	Mode        string                   `yaml:"mode"`
	Protected   bool                     `yaml:"protected"`
	Complete    bool                     `yaml:"listing_complete"`
	WriterHeld  bool                     `yaml:"writer_active"`
	Classes     map[string]RemoteGCBytes `yaml:"classes"`
	Generations []RemoteGCGeneration     `yaml:"generations,omitempty"`
	Truncated   bool                     `yaml:"generations_truncated,omitempty"`
	Quarantined int                      `yaml:"quarantined"`
	Deleted     int                      `yaml:"deleted"`
	Kept        int                      `yaml:"kept"`
}

type gcGeneration struct {
	data            []objectstore.Entry
	registry        bool
	marker          bool
	objects         int
	bytes           int64
	class, action   string
	referencedClass string
}

type remoteCollector struct {
	backend  *objectBackend
	data     objectstore.Client
	list     objectstore.Collector
	registry *readerlease.Registry // nil for legacy namespaces
	regDel   objectstore.Collector
	options  RemoteGCOptions
}

type gcMarker struct {
	Version       int       `yaml:"version"`
	Generation    string    `yaml:"generation"`
	Incarnation   string    `yaml:"incarnation"`
	QuarantinedAt time.Time `yaml:"quarantined_at"`
	// QuiescentSince is the provider time of the first quiescent inspection.
	// Grace runs from here, not from quarantine, so a long-lived reader that
	// expires late still gets the full grace before deletion.
	QuiescentSince time.Time `yaml:"quiescent_since,omitempty"`
}

func (c *remoteCollector) run(ctx context.Context, dataset string) (RemoteGCReport, error) {
	report := RemoteGCReport{Dataset: dataset, Mode: "dry-run", Protected: c.backend.protected(), Classes: map[string]RemoteGCBytes{}}
	if c.options.Delete {
		report.Mode = "delete"
		if c.options.Grace < MinRemoteGCGrace {
			return report, errors.New("remote collection grace must be at least 10 minutes")
		}
		if !report.Protected || c.registry == nil || c.regDel == nil {
			return report, ErrProtectionRequired
		}
	}
	if err := c.backend.check(ctx, dataset); err != nil {
		return report, err
	}
	// List before reading the manifest: every listed object then existed before
	// the manifest read, so its writer is either visible, published or gone.
	gens, complete, err := c.inventory(ctx, dataset, &report)
	if err != nil {
		return report, err
	}
	report.Complete = complete
	state, err := c.backend.readState(ctx, dataset, c.data)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return report, err
	}
	referenced := referencedGenerations(state.manifest)
	now := state.now()
	writer := state.manifest.Writer
	report.WriterHeld = writer != nil && !writer.ExpiresAt.Add(remoteGCWriterMargin).Before(now)
	names := make([]string, 0, len(gens))
	for name, g := range gens {
		names = append(names, name)
		switch {
		case referenced[name] != "":
			g.class = referenced[name]
		case report.WriterHeld || (writer != nil && writer.Owner == name):
			g.class = "staging"
		case g.marker:
			g.class = "quarantined"
		default:
			g.class = "orphan"
		}
		g.action = "kept"
	}
	sort.Strings(names)
	for _, name := range names {
		g := gens[name]
		if c.options.Delete && complete && !report.WriterHeld && (g.class == "orphan" || g.class == "quarantined") {
			g.action = c.collect(ctx, dataset, name, g)
		}
		switch g.action {
		case "quarantined":
			report.Quarantined++
		case "deleted":
			report.Deleted++
		default:
			report.Kept++
		}
		total := report.Classes[g.class]
		total.Objects += int64(g.objects)
		total.Bytes += g.bytes
		report.Classes[g.class] = total
		if len(report.Generations) < remoteGCReportLimit {
			report.Generations = append(report.Generations, RemoteGCGeneration{Generation: name, Class: g.class, Objects: g.objects, Bytes: g.bytes, Action: g.action})
		} else {
			report.Truncated = true
		}
	}
	return report, ctx.Err()
}

func referencedGenerations(m objectManifest) map[string]string {
	refs := map[string]string{}
	for _, h := range m.History {
		if h != nil {
			refs[h.Generation] = "retained"
		}
	}
	if m.Committed != nil {
		refs[m.Committed.Generation] = "current"
	}
	return refs
}

// inventory pages the dataset prefix with bounded memory. An incomplete or
// oversized listing is reported but never authorizes deletion.
func (c *remoteCollector) inventory(ctx context.Context, dataset string, report *RemoteGCReport) (map[string]*gcGeneration, bool, error) {
	prefix := c.backend.key(dataset, "")
	gens := map[string]*gcGeneration{}
	after, seen := "", 0
	for {
		page, err := c.list.List(ctx, prefix, after, objectstore.MaxListPage)
		if err != nil {
			return nil, false, err
		}
		for _, entry := range page.Entries {
			seen++
			name := strings.TrimPrefix(entry.Key, prefix)
			var generation string
			kind := ""
			if m := gcDataName.FindStringSubmatch(name); m != nil {
				generation, kind = m[1], "data"
			} else if m := gcRegistryName.FindStringSubmatch(name); m != nil {
				generation, kind = m[1], "registry"
			} else if m := gcMarkerName.FindStringSubmatch(name); m != nil {
				generation, kind = m[1], "marker"
			} else {
				class := "unknown"
				if name == storeManifestName {
					class = "manifest"
				}
				total := report.Classes[class]
				total.Objects++
				total.Bytes += entry.Size
				report.Classes[class] = total
				continue
			}
			g := gens[generation]
			if g == nil {
				if len(gens) >= remoteGCMaxGenerations {
					return gens, false, nil
				}
				g = &gcGeneration{}
				gens[generation] = g
			}
			g.objects++
			g.bytes += entry.Size
			switch kind {
			case "data":
				g.data = append(g.data, entry)
			case "registry":
				g.registry = true
			case "marker":
				g.marker = true
			}
		}
		if page.Next == "" {
			return gens, true, nil
		}
		if seen >= remoteGCMaxObjects || page.Next == after {
			return gens, false, nil
		}
		after = page.Next
	}
}

// collect returns the action taken. Order: marker, retire, grace, recheck,
// quiescence, data, registry document, marker. A crash after any step leaves
// either an unreferenced retired generation or a marker that the next run
// resumes from; no reader can newly acquire a retired generation.
func (c *remoteCollector) collect(ctx context.Context, dataset, generation string, g *gcGeneration) string {
	markerKey := c.backend.key(dataset, "gc/"+generation+".quarantine.yaml")
	if !g.registry {
		if g.marker && len(g.data) == 0 {
			// Only a marker remains from an interrupted collection.
			if c.deleteExact(ctx, c.list, c.data, markerKey) == nil {
				return "deleted"
			}
		}
		return "kept: no registry document"
	}
	ref, err := c.registry.Discover(ctx, dataset, generation)
	if err != nil {
		return "kept: registry unreadable"
	}
	marker, markerVersion, err := c.readMarker(ctx, markerKey, generation)
	if errors.Is(err, objectstore.ErrNotFound) {
		marker, err = c.writeMarker(ctx, dataset, markerKey, generation, ref)
		if errors.Is(err, errGCRecheck) {
			return "kept: referenced or writer active"
		}
		if errors.Is(err, readerlease.ErrClock) {
			return "kept: clock"
		}
		if err != nil {
			return "kept: quarantine write failed"
		}
		if c.registry.Retire(ctx, ref) != nil {
			return "kept: retire failed"
		}
		return "quarantined"
	}
	if err != nil || marker.Incarnation != ref.Incarnation {
		return "kept: quarantine marker invalid"
	}
	if c.registry.Retire(ctx, ref) != nil {
		return "kept: retire failed"
	}
	// Recheck on a fresh manifest: the generation must still be unreferenced,
	// no writer may be active, and the grace must have passed on provider time.
	state, err := c.backend.readState(ctx, dataset, c.data)
	if err != nil {
		return "kept: manifest unreadable"
	}
	if state.info.ServerTime.IsZero() {
		return "kept: clock"
	}
	if gcBlocked(state, generation) {
		return "kept: referenced or writer active"
	}
	inspection, err := c.registry.Inspect(ctx, ref)
	if err != nil || !inspection.Quiescent() {
		return "kept: readers may be live"
	}
	if marker.QuiescentSince.IsZero() {
		marker.QuiescentSince = state.now()
		if c.putMarker(ctx, markerKey, marker, objectstore.Condition{Version: markerVersion}) != nil {
			return "kept: quarantine write failed"
		}
		return "quarantined"
	}
	if state.now().Sub(marker.QuiescentSince) < c.options.Grace {
		return "quarantined"
	}
	for _, entry := range g.data {
		if c.deleteExact(ctx, c.list, c.data, entry.Key) != nil {
			return "kept: partial deletion, will resume"
		}
	}
	key, err := c.registry.DocumentKey(ref)
	if err != nil {
		return "kept: registry key invalid"
	}
	if err := c.regDel.Delete(ctx, key, inspection.Version); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return "kept: registry deletion uncertain, will resume"
	}
	if c.deleteExact(ctx, c.list, c.data, markerKey) != nil {
		return "kept: marker deletion uncertain, will resume"
	}
	return "deleted"
}

// deleteExact heads for the current version and deletes only that version.
// Keys are immutable generation names, so a changed version is a conflict.
func (c *remoteCollector) deleteExact(ctx context.Context, del objectstore.Collector, head objectstore.Client, key string) error {
	info, err := head.Head(ctx, key, "")
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := del.Delete(ctx, key, info.Version); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return err
	}
	return nil
}

var errGCRecheck = errors.New("generation became referenced or a writer is active")

// gcBlocked is true when the generation is referenced or a writer lease is
// not provably expired by the margin on the given provider-clock state.
func gcBlocked(state objectState, generation string) bool {
	if referencedGenerations(state.manifest)[generation] != "" {
		return true
	}
	w := state.manifest.Writer
	return w != nil && !w.ExpiresAt.Add(remoteGCWriterMargin).Before(state.now())
}

func (c *remoteCollector) readMarker(ctx context.Context, key, generation string) (gcMarker, string, error) {
	marker, info, err := c.readMarkerInfo(ctx, key, generation)
	return marker, info.Version, err
}

func (c *remoteCollector) readMarkerInfo(ctx context.Context, key, generation string) (gcMarker, objectstore.Info, error) {
	body, info, err := c.data.Get(ctx, key, "")
	if err != nil {
		return gcMarker{}, info, err
	}
	defer body.Close()
	if info.Size <= 0 || info.Size > remoteGCMarkerLimit {
		return gcMarker{}, info, ErrCorrupt
	}
	raw, err := io.ReadAll(io.LimitReader(body, remoteGCMarkerLimit+1))
	if err != nil || int64(len(raw)) != info.Size || (info.SHA256 != "" && info.SHA256 != objectSHA256(raw)) {
		return gcMarker{}, info, ErrCorrupt
	}
	var marker gcMarker
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(&marker) != nil || marker.Version != 1 || marker.Generation != generation || marker.QuarantinedAt.IsZero() {
		return gcMarker{}, info, ErrCorrupt
	}
	return marker, info, nil
}

// The quarantine time comes from the provider clock observed with the manifest.
// It re-checks the fresh manifest so a writer CAS that landed after
// classification stops quarantine before Retire.
func (c *remoteCollector) writeMarker(ctx context.Context, dataset, key, generation string, ref readerlease.Reference) (gcMarker, error) {
	state, err := c.backend.readState(ctx, dataset, c.data)
	if err != nil {
		return gcMarker{}, err
	}
	if state.info.ServerTime.IsZero() {
		return gcMarker{}, readerlease.ErrClock
	}
	if gcBlocked(state, generation) {
		return gcMarker{}, errGCRecheck
	}
	marker := gcMarker{Version: 1, Generation: ref.Generation, Incarnation: ref.Incarnation, QuarantinedAt: state.now()}
	return marker, c.putMarker(ctx, key, marker, objectstore.Condition{Absent: true})
}

func (c *remoteCollector) putMarker(ctx context.Context, key string, marker gcMarker, condition objectstore.Condition) error {
	raw, err := yaml.Marshal(marker)
	if err != nil {
		return err
	}
	if _, err := c.data.Put(ctx, key, bytes.NewReader(raw), int64(len(raw)), objectSHA256(raw), condition); err != nil {
		return fmt.Errorf("quarantine marker: %w", err)
	}
	return nil
}

// RunRemoteGC reconciles one dataset and, only with options.Delete in a
// protected namespace, quarantines and collects unreferenced generations.
// Data listing/deletion uses the writer identity and registry documents use
// the registry identity; both need list/delete grants only when Delete is set.
func RunRemoteGC(ctx context.Context, c catalog.Config, dataset string, options RemoteGCOptions) (RemoteGCReport, error) {
	report := RemoteGCReport{Dataset: dataset, Mode: "dry-run"}
	if c.Acceleration == nil || c.Acceleration.ObjectStorage == nil {
		return report, errors.New("remote collection requires object storage")
	}
	storage := c.Acceleration.ObjectStorage
	data, err := objectstore.New(storage.ObjectLocation, storage.WriteCredentials)
	if err != nil {
		return report, err
	}
	defer data.Close()
	list, ok := data.(objectstore.Collector)
	if !ok {
		return report, objectstore.ErrUnsupported
	}
	backend, err := newObjectBackend(*c.Acceleration, data)
	if err != nil {
		return report, err
	}
	collector := &remoteCollector{backend: backend, data: data, list: list, options: options}
	if storage.ReaderRegistry != nil {
		registryClient, err := objectstore.New(storage.ObjectLocation, storage.ReaderRegistry.Credentials)
		if err != nil {
			return report, err
		}
		defer registryClient.Close()
		store, err := readerlease.NewObjectStore(registryClient, storage.Prefix, c.Acceleration.TenantID)
		if err != nil {
			return report, err
		}
		if collector.registry, err = readerlease.New(store, readerlease.DefaultConfig(storage.Prefix, c.Acceleration.TenantID)); err != nil {
			return report, err
		}
		collector.regDel, _ = registryClient.(objectstore.Collector)
	}
	return collector.run(ctx, dataset)
}
