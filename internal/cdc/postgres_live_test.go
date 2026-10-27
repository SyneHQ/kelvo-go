package cdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Live tests need a PostgreSQL superuser DSN (tested on 17) with wal_level=logical in
// KELVO_TEST_PG_CDC_DSN. They run sequentially; each uses its own schema,
// publication and slot.

type memSink struct {
	mu      sync.Mutex
	rows    map[string]map[string][]any
	applies map[uint64]int
	batches []Batch
	failN   atomic.Int32 // fail the next N applies before applying
}

func (s *memSink) Apply(_ context.Context, b Batch) error {
	if s.failN.Add(-1) >= 0 {
		return errors.New("injected sink crash")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.Reset || s.rows == nil {
		s.rows, s.applies = map[string]map[string][]any{}, map[uint64]int{}
	}
	for _, c := range b.Changes {
		t := s.rows[c.Table]
		if t == nil {
			t = map[string][]any{}
			s.rows[c.Table] = t
		}
		k := fmt.Sprint(c.Key...)
		switch c.Op {
		case OpInsert, OpUpdate:
			row := slices.Clone(c.Row)
			for i, v := range row {
				if v == Unchanged {
					old, ok := t[k]
					if !ok {
						return fmt.Errorf("unchanged value for missing row %s", k)
					}
					row[i] = old[i]
				}
			}
			t[k] = row
		case OpDelete:
			delete(t, k)
		case OpTruncate:
			delete(s.rows, c.Table)
		}
	}
	if !b.Snapshot {
		s.applies[b.LSN]++
	}
	s.batches = append(s.batches, b)
	return nil
}

func (s *memSink) has(table string, k ...any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[table][fmt.Sprint(k...)]
	return ok
}

// faultStore injects a crash before (failBefore) or after (failAfter) a save.
type faultStore struct {
	FileStore
	failBefore, failAfter atomic.Int32
}

func (f *faultStore) Save(ctx context.Context, cp Checkpoint) error {
	if f.failBefore.Add(-1) >= 0 {
		return errors.New("injected crash before checkpoint")
	}
	if err := f.FileStore.Save(ctx, cp); err != nil {
		return err
	}
	if f.failAfter.Add(-1) >= 0 {
		return errors.New("injected crash after checkpoint, before ACK")
	}
	return nil
}

type env struct {
	t      *testing.T
	ctx    context.Context
	db     *pgx.Conn
	schema string
	p      *Postgres
	sink   *memSink
	store  *faultStore
	marker int
}

func newEnv(t *testing.T, ddl ...string) *env {
	dsn := os.Getenv("KELVO_TEST_PG_CDC_DSN")
	if dsn == "" {
		t.Skip("KELVO_TEST_PG_CDC_DSN not set")
	}
	ctx := context.Background()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "w8cdc_" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_"))
	e := &env{t: t, ctx: ctx, db: db, schema: name, sink: &memSink{},
		store: &faultStore{FileStore: FileStore{Path: filepath.Join(t.TempDir(), "checkpoint.json")}},
		p:     &Postgres{DSN: dsn, Slot: name, Publication: name, StatusInterval: 200 * time.Millisecond}}
	e.cleanup()
	e.exec("CREATE SCHEMA " + name)
	e.exec("SET search_path = " + name)
	e.exec("CREATE TABLE marker (id int PRIMARY KEY)")
	for _, d := range ddl {
		e.exec(d)
	}
	e.exec("CREATE PUBLICATION " + name + " FOR TABLES IN SCHEMA " + name)
	t.Cleanup(func() { e.cleanup(); db.Close(ctx) })
	return e
}

func (e *env) cleanup() {
	for range 50 {
		if err := e.p.DropSlot(e.ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.db.Exec(e.ctx, "DROP PUBLICATION IF EXISTS "+e.schema)
	e.db.Exec(e.ctx, "DROP SCHEMA IF EXISTS "+e.schema+" CASCADE")
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Exec(e.ctx, sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

// run starts Run, retrying while the previous walsender still holds the slot.
func (e *env) run(ctx context.Context) error {
	for {
		err := e.p.Run(ctx, e.sink, e.store)
		if err == nil || !strings.Contains(err.Error(), "is active for PID") || ctx.Err() != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runToError runs until Run fails (an injected crash or a stop condition).
func (e *env) runToError() error {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()
	err := e.run(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		e.t.Fatalf("Run did not fail: %v", err)
	}
	return err
}

// drain streams until a fresh marker row arrives and the slot's confirmed
// flush position equals the checkpoint, then stops Run.
func (e *env) drain() {
	e.t.Helper()
	e.marker++
	e.exec(fmt.Sprintf("INSERT INTO %s.marker VALUES (%d)", e.schema, e.marker))
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan error, 1)
	go func() { done <- e.run(ctx) }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case err := <-done:
			cancel()
			e.t.Fatalf("Run stopped: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			cancel()
			e.t.Fatal("drain timed out")
		}
		if !e.sink.has(e.schema+".marker", int32(e.marker)) {
			continue
		}
		cp, _, _ := e.store.Load(e.ctx)
		if e.confirmed() == cp.LSN {
			break
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		e.t.Fatalf("Run: %v", err)
	}
}

func (e *env) confirmed() uint64 {
	var s string
	if err := e.db.QueryRow(e.ctx, "SELECT coalesce(confirmed_flush_lsn,'0/0')::text FROM pg_replication_slots WHERE slot_name=$1", e.p.Slot).Scan(&s); err != nil {
		e.t.Fatal(err)
	}
	l, _ := pglogrepl.ParseLSN(s)
	return uint64(l)
}

func (e *env) bootstrap() {
	e.t.Helper()
	if err := e.p.Bootstrap(e.ctx, e.sink, e.store); err != nil {
		e.t.Fatalf("bootstrap: %v", err)
	}
}

// assertMatches compares the sink with the source tables read the same way.
func (e *env) assertMatches() {
	e.t.Helper()
	tx, err := e.db.Begin(e.ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	defer tx.Rollback(e.ctx)
	for _, k := range slices.Collect(func(yield func(string) bool) {
		for k, v := range sessionParams {
			if !yield("SET LOCAL " + k + " = '" + v + "'") {
				return
			}
		}
	}) {
		if _, err := tx.Exec(e.ctx, k); err != nil {
			e.t.Fatal(err)
		}
	}
	schemas, err := e.p.loadSchemas(e.ctx, tx)
	if err != nil {
		e.t.Fatal(err)
	}
	want := map[string]map[string][]any{}
	for _, s := range schemas {
		rows := map[string][]any{}
		if err := readTable(e.ctx, tx.Conn().PgConn(), s, func(r []any) error { rows[fmt.Sprint(key(s, r)...)] = r; return nil }); err != nil {
			e.t.Fatal(err)
		}
		if len(rows) > 0 {
			want[s.Table()] = rows
		}
	}
	e.sink.mu.Lock()
	defer e.sink.mu.Unlock()
	got := map[string]map[string][]any{}
	for t, rows := range e.sink.rows {
		if len(rows) > 0 {
			got[t] = rows
		}
	}
	if !reflect.DeepEqual(got, want) {
		for t := range want {
			if !reflect.DeepEqual(got[t], want[t]) {
				e.t.Errorf("%s: sink %d rows, source %d rows", t, len(got[t]), len(want[t]))
			}
		}
		e.t.Fatal("sink does not match source")
	}
}

func (e *env) duplicates() int {
	e.sink.mu.Lock()
	defer e.sink.mu.Unlock()
	n := 0
	for _, c := range e.sink.applies {
		n += c - 1
	}
	return n
}

func TestPGCDCSnapshotHandoffConcurrentWrites(t *testing.T) {
	e := newEnv(t, "CREATE TABLE items (id bigint PRIMARY KEY, v text, n numeric)",
		"INSERT INTO items SELECT g, 'seed', g * 1.5 FROM generate_series(1, 2000) g")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var written atomic.Int64
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(e.ctx, e.p.DSN)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close(e.ctx)
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := 10_000 + int64(w)*1_000_000 + i
				err := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
					for _, q := range []string{"INSERT INTO %s.items VALUES ($1::bigint, 'w', $1::bigint * 0.25)",
						"UPDATE %s.items SET v = 'u' || $1::bigint WHERE id = ($1::bigint % 2000) + 1", "DELETE FROM %s.items WHERE id = $1::bigint - 3"} {
						if _, err := tx.Exec(e.ctx, strings.Replace(q, "%s", e.schema, 1), id); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				written.Add(1)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	e.bootstrap()
	before := written.Load()
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()
	e.drain()
	e.assertMatches()
	t.Logf("handoff: %d writer txns, %d during bootstrap+stream, duplicates %d", written.Load(), written.Load()-before, e.duplicates())
	if e.duplicates() != 0 {
		t.Fatalf("clean run applied %d duplicate transactions", e.duplicates())
	}
}

func TestPGCDCTypesKeyChangeTruncate(t *testing.T) {
	e := newEnv(t, `CREATE TABLE typed (id int PRIMARY KEY, s smallint, b bigint, n numeric(30,10), f float8, r real,
ts timestamptz, tl timestamp, d date, t text, vc varchar(5), bp char(3), by bytea, ok bool, u uuid, j jsonb)`,
		"CREATE TABLE other (k text PRIMARY KEY)")
	e.bootstrap()
	e.exec(`INSERT INTO ` + e.schema + `.typed VALUES (1, -32768, 9223372036854775807, 12345678901234567890.0123456789, 0.1, 1.5,
'2024-02-29 23:59:59.123456+05:30', '2024-02-29 23:59:59.123456', '2024-02-29', 'x', 'abc', 'a', '\x00ff', true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": 1}'),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	e.exec("UPDATE " + e.schema + ".typed SET id = 100 WHERE id = 1")
	e.exec("INSERT INTO " + e.schema + ".other VALUES ('a'), ('b')")
	e.drain()
	e.assertMatches()
	var keyChange []Op
	for _, b := range e.sink.batches {
		if len(b.Changes) == 2 && b.Changes[0].Table == e.schema+".typed" && b.Changes[0].Op == OpDelete {
			keyChange = []Op{b.Changes[0].Op, b.Changes[1].Op}
		}
	}
	if !slices.Equal(keyChange, []Op{OpDelete, OpInsert}) {
		t.Fatalf("key change = %v, want [delete insert]", keyChange)
	}
	row := e.sink.rows[e.schema+".typed"][fmt.Sprint(int32(100))]
	want := []any{int32(100), int16(-32768), int64(9223372036854775807), Numeric("12345678901234567890.0123456789"), 0.1, float32(1.5),
		time.Date(2024, 2, 29, 18, 29, 59, 123456000, time.UTC), time.Date(2024, 2, 29, 23, 59, 59, 123456000, time.UTC),
		time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), "x", "abc", "a  ", []byte{0, 255}, true, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", `{"a": 1}`}
	if !reflect.DeepEqual(row, want) {
		t.Fatalf("typed row\n got %#v\nwant %#v", row, want)
	}
	if nulls := e.sink.rows[e.schema+".typed"][fmt.Sprint(int32(2))]; slices.ContainsFunc(nulls[1:], func(v any) bool { return v != nil }) {
		t.Fatalf("NULL row = %#v", nulls)
	}
	e.exec("TRUNCATE " + e.schema + ".other")
	e.drain()
	last := e.sink.batches[len(e.sink.batches)-2] // before the marker transaction
	if len(last.Changes) != 1 || last.Changes[0].Op != OpTruncate {
		t.Fatalf("truncate batch = %+v", last.Changes)
	}
	e.assertMatches()
}

func TestPGCDCToastUnchanged(t *testing.T) {
	e := newEnv(t, "CREATE TABLE docs (id int PRIMARY KEY, body text, v int)",
		"ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL",
		"INSERT INTO docs SELECT g, (SELECT string_agg(md5(g::text||i::text), '') FROM generate_series(1, 400) i), 0 FROM generate_series(1, 3) g")
	e.bootstrap()
	e.exec("UPDATE " + e.schema + ".docs SET v = v + 1")
	e.drain()
	seen := 0
	for _, b := range e.sink.batches {
		for _, c := range b.Changes {
			if c.Op == OpUpdate && c.Row[1] == Unchanged {
				seen++
			}
		}
	}
	if seen != 3 {
		t.Fatalf("unchanged TOAST updates = %d, want 3", seen)
	}
	e.assertMatches()
}

func TestPGCDCLargeTransaction(t *testing.T) {
	e := newEnv(t, "CREATE TABLE big (id int PRIMARY KEY, v text)")
	e.bootstrap()
	e.exec("INSERT INTO " + e.schema + ".big SELECT g, repeat('x', 100) FROM generate_series(1, 50000) g")
	e.drain()
	if n := len(e.sink.batches[len(e.sink.batches)-2].Changes); n != 50000 {
		t.Fatalf("large transaction batch has %d changes", n)
	}
	e.assertMatches()
	e.p.MaxTxnChanges = 1000
	e.exec("UPDATE " + e.schema + ".big SET v = 'y'")
	if err := e.runToError(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("over-limit transaction: %v", err)
	}
}

func TestPGCDCCrashBoundaries(t *testing.T) {
	e := newEnv(t, "CREATE TABLE acct (id int PRIMARY KEY, bal numeric)", "INSERT INTO acct SELECT g, 100 FROM generate_series(1, 10) g")
	e.bootstrap()
	tx := func(i int) { e.exec(fmt.Sprintf("UPDATE %s.acct SET bal = bal + %d WHERE id = %d", e.schema, i, i%10+1)) }

	// Crash inside apply: nothing applied, nothing checkpointed.
	tx(1)
	e.sink.failN.Store(1)
	t.Logf("crash during apply: %v", e.runToError())
	e.drain()
	e.assertMatches()
	if d := e.duplicates(); d != 0 {
		t.Fatalf("duplicates after apply crash = %d", d)
	}

	// Crash after apply, before checkpoint: the transaction is replayed.
	tx(2)
	e.store.failBefore.Store(1)
	cpBefore, _, _ := e.store.Load(e.ctx)
	t.Logf("crash between apply and checkpoint: %v", e.runToError())
	if cp, _, _ := e.store.Load(e.ctx); cp.LSN != cpBefore.LSN {
		t.Fatal("checkpoint advanced despite injected crash")
	}
	e.drain()
	e.assertMatches()
	if d := e.duplicates(); d != 1 {
		t.Fatalf("replayed transactions = %d, want 1 idempotent replay", d)
	}

	// Crash after checkpoint, before ACK: the source resends, Run skips it.
	tx(3)
	e.store.failAfter.Store(1)
	t.Logf("crash between checkpoint and ACK: %v", e.runToError())
	cp, _, _ := e.store.Load(e.ctx)
	if c := e.confirmed(); c >= cp.LSN {
		t.Fatalf("source acknowledged %s before restart; checkpoint %s", pglogrepl.LSN(c), pglogrepl.LSN(cp.LSN))
	}
	e.drain()
	e.assertMatches()
	if d := e.duplicates(); d != 1 {
		t.Fatalf("duplicates after ACK crash = %d, want still 1", d)
	}

	// Clean stop and restart resumes exactly.
	for i := range 20 {
		tx(10 + i)
		if i == 9 {
			e.drain()
		}
	}
	e.drain()
	e.assertMatches()
	if d := e.duplicates(); d != 1 {
		t.Fatalf("duplicates after clean restart = %d", d)
	}
}

func TestPGCDCSchemaChange(t *testing.T) {
	e := newEnv(t, "CREATE TABLE evo (id int PRIMARY KEY, a text)")
	e.bootstrap()
	e.exec("ALTER TABLE " + e.schema + ".evo ADD COLUMN b numeric")
	e.exec("INSERT INTO " + e.schema + ".evo VALUES (1, 'x', 2.50)")
	e.drain()
	var version int
	for _, b := range e.sink.batches {
		for _, s := range b.Schemas {
			if s.Name == "evo" {
				version = s.Version
			}
		}
	}
	if version != 2 {
		t.Fatalf("schema version event = %d, want 2", version)
	}
	e.exec("INSERT INTO " + e.schema + ".evo VALUES (2, 'y', 3)")
	n := len(e.sink.batches)
	e.drain()
	for _, b := range e.sink.batches[n:] {
		for _, s := range b.Schemas {
			if s.Name == "evo" {
				t.Fatalf("schema event repeated after restart: %+v", s)
			}
		}
	}
	e.assertMatches()
	e.exec("ALTER TABLE " + e.schema + ".evo ALTER COLUMN a TYPE varchar(10)")
	e.exec("INSERT INTO " + e.schema + ".evo VALUES (3, 'z', 4)")
	if err := e.runToError(); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("incompatible change: %v", err)
	}
}

func TestPGCDCBootstrapRequired(t *testing.T) {
	e := newEnv(t, "CREATE TABLE t (id int PRIMARY KEY)")
	if err := e.p.Run(e.ctx, e.sink, e.store); !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("no checkpoint: %v", err)
	}
	e.bootstrap()
	stale, _, _ := e.store.Load(e.ctx)
	e.exec("INSERT INTO " + e.schema + ".t VALUES (1)")
	e.drain()
	if err := e.store.FileStore.Save(e.ctx, stale); err != nil {
		t.Fatal(err)
	}
	err := e.p.Run(e.ctx, e.sink, e.store)
	t.Logf("stale checkpoint: %v", err)
	if !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("stale checkpoint: %v", err)
	}
	e.bootstrap()
	e.drain()

	// WAL removed past the slot: invalidated slot.
	e.exec("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	e.exec("SELECT pg_reload_conf()")
	t.Cleanup(func() {
		e.db.Exec(e.ctx, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		e.db.Exec(e.ctx, "SELECT pg_reload_conf()")
	})
	e.exec("CREATE TABLE public." + e.schema + "_fill (v text)")
	t.Cleanup(func() { e.db.Exec(e.ctx, "DROP TABLE IF EXISTS public."+e.schema+"_fill") })
	status := ""
	for i := 0; i < 20 && status != "lost"; i++ {
		e.exec("INSERT INTO public." + e.schema + "_fill SELECT repeat('x', 1000) FROM generate_series(1, 5000)")
		e.exec("SELECT pg_switch_wal()")
		e.exec("CHECKPOINT")
		e.db.QueryRow(e.ctx, "SELECT coalesce(wal_status,'') FROM pg_replication_slots WHERE slot_name=$1", e.p.Slot).Scan(&status)
	}
	err = e.p.Run(e.ctx, e.sink, e.store)
	t.Logf("wal removed (wal_status=%s): %v", status, err)
	if !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("invalidated slot: %v", err)
	}

	e.bootstrap()
	if err := e.p.DropSlot(e.ctx); err != nil {
		t.Fatal(err)
	}
	err = e.p.Run(e.ctx, e.sink, e.store)
	t.Logf("dropped slot: %v", err)
	if !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("dropped slot: %v", err)
	}
}

func TestPGCDCValidation(t *testing.T) {
	for name, ddl := range map[string][]string{
		"full":     {"CREATE TABLE x (id int PRIMARY KEY)", "ALTER TABLE x REPLICA IDENTITY FULL"},
		"nothing":  {"CREATE TABLE x (id int PRIMARY KEY)", "ALTER TABLE x REPLICA IDENTITY NOTHING"},
		"nokey":    {"CREATE TABLE x (id int)"},
		"type":     {"CREATE TABLE x (id int PRIMARY KEY, p point)"},
		"interval": {"CREATE TABLE x (id int PRIMARY KEY, i interval)"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, ddl...)
			err := e.p.Bootstrap(e.ctx, e.sink, e.store)
			t.Logf("%v", err)
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("bootstrap: %v", err)
			}
		})
	}
	t.Run("no_truncate", func(t *testing.T) {
		e := newEnv(t, "CREATE TABLE x (id int PRIMARY KEY)")
		e.exec("ALTER PUBLICATION " + e.schema + " SET (publish = 'insert, update, delete')")
		if err := e.p.Validate(e.ctx); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("validate: %v", err)
		}
	})
}
