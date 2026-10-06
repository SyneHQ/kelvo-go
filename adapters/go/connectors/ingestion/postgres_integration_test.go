package ingestion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	wire "github.com/SYNEHQ/kelvo-go/ingestion"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func testStore(t *testing.T) Store {
	t.Helper()
	dsn := os.Getenv("INGESTION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("INGESTION_POSTGRES_TEST_DSN not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/commerce_validation" {
		t.Fatal("use disposable loopback commerce_validation database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	scope := fixtureScope()
	scope.Schema = fmt.Sprintf("ingestion_test_%d", time.Now().UnixNano())
	s := Store{DB: db, Scope: scope}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Install(ctx); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DROP SCHEMA "` + scope.Schema + `" CASCADE`); db.Close() })
	return s
}
func countRows(t *testing.T, s Store, table string) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow(`SELECT count(*) FROM ` + s.name(table)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgresLostAckCorrectionsAndResume(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	batch := fixtureBatch()
	first, err := s.Commit(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	// Discard the acknowledgement and reconstruct the destination as a new process.
	resumed := Store{DB: s.DB, Scope: s.Scope}
	again, err := resumed.Commit(ctx, batch)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("lost-ack retry: %v %#v %#v", err, first, again)
	}
	changed := fixtureBatch()
	changed.Records[0].Payload = json.RawMessage(`{"amount":"1.00"}`)
	if _, err = resumed.Commit(ctx, changed); !errors.Is(err, wire.ErrConflict) {
		t.Fatal("changed retry accepted", err)
	}
	changed.ID = "batch-2"
	changed.ExpectedSequence = 1
	if _, err = resumed.Commit(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if _, err = resumed.Commit(ctx, batch); err != nil {
		t.Fatal("old identical receipt should remain readable", err)
	}
	if countRows(t, s, "records") != 1 || countRows(t, s, "versions") != 2 || countRows(t, s, "batches") != 2 {
		t.Fatal("duplicate or missing durable records")
	}
	var original string
	if err = s.DB.QueryRow(`SELECT original_json FROM ` + s.name("versions") + ` WHERE batch_id='batch-1'`).Scan(&original); err != nil || original != string(batch.Records[0].Payload) {
		t.Fatal("lost exact source representation", err)
	}
	tombstone := fixtureBatch()
	tombstone.ID = "batch-3"
	tombstone.ExpectedSequence = 2
	tombstone.Records[0].Deleted = true
	tombstone.Records[0].Payload = json.RawMessage(`{}`)
	if _, err = s.Commit(ctx, tombstone); err != nil {
		t.Fatal(err)
	}
	var deleted bool
	if err = s.DB.QueryRow(`SELECT deleted FROM ` + s.name("records")).Scan(&deleted); err != nil || !deleted {
		t.Fatal("missing tombstone", err)
	}
	state, err := s.State(ctx)
	if err != nil || state.Sequence != 3 || state.LastReceipt.BatchID != "batch-3" {
		t.Fatalf("bad state: %+v %v", state, err)
	}
	if countRows(t, s, "versions") != 3 {
		t.Fatal("tombstone destroyed history")
	}
}

func TestPostgresRollbackAndCancellation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.DB.Exec(`ALTER TABLE ` + s.name("versions") + ` ADD CHECK(record_id <> 'fail')`); err != nil {
		t.Fatal(err)
	}
	batch := fixtureBatch()
	batch.Records = append(batch.Records, wire.Record{ID: "fail", Payload: json.RawMessage(`{}`)})
	if _, err := s.Commit(ctx, batch); err == nil {
		t.Fatal("constraint failure accepted")
	}
	state, err := s.State(ctx)
	if err != nil || state.Sequence != 0 || countRows(t, s, "records") != 0 || countRows(t, s, "batches") != 0 {
		t.Fatal("partial commit", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Commit(cancelled, fixtureBatch()); err == nil {
		t.Fatal("cancelled request committed")
	}
	if _, err := s.Commit(ctx, fixtureBatch()); err != nil {
		t.Fatal("retry after rollback failed", err)
	}
}

func TestPostgresConcurrentWritersAndScopeBoundaries(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := fixtureBatch()
			b.ID = fmt.Sprintf("concurrent-%d", i)
			_, err := s.Commit(ctx, b)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, wire.ErrConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("writers did not serialize")
	}
	foreign := s
	foreign.Scope.TeamID = "team-b"
	if _, err := foreign.State(ctx); !errors.Is(err, wire.ErrUninitialized) {
		t.Fatal("cross-tenant state", err)
	}
	if _, err := foreign.Commit(ctx, fixtureBatch()); !errors.Is(err, wire.ErrUninitialized) {
		t.Fatal("cross-tenant commit", err)
	}
	if err := foreign.Install(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Commit(ctx, fixtureBatch()); err != nil {
		t.Fatal(err)
	}
	changed := s
	changed.Scope.Binding = strings.Repeat("b", 64)
	if err := changed.Install(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := changed.Commit(ctx, fixtureBatch()); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "records") != 3 {
		t.Fatal("tenant or configuration generations collided")
	}
	wrong := s
	wrong.Scope.Database = "other_database"
	if err := wrong.Install(ctx); !errors.Is(err, wire.ErrDatabase) {
		t.Fatal("wrong database install", err)
	}
	if _, err := wrong.Commit(ctx, fixtureBatch()); !errors.Is(err, wire.ErrDatabase) {
		t.Fatal("wrong database commit", err)
	}
}

func TestPostgresEmptyPageAndBoundedVolume(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	start := time.Now()
	totalBytes := 0
	for page := 0; page < 10; page++ {
		b := fixtureBatch()
		b.ID = fmt.Sprintf("volume-%d", page)
		b.ExpectedSequence = int64(page)
		b.Records = make([]wire.Record, 1000)
		for row := range b.Records {
			b.Records[row] = wire.Record{ID: fmt.Sprintf("row-%d", page*1000+row), Payload: json.RawMessage(`{"amount":"19.99","currency":"INR","description":"bounded fixture"}`)}
		}
		encoded, _ := json.Marshal(b)
		totalBytes += len(encoded)
		if _, err := s.Commit(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	empty := fixtureBatch()
	empty.ID = "terminal"
	empty.ExpectedSequence = 10
	empty.Records = nil
	empty.Checkpoint = json.RawMessage(`{"done":true}`)
	if _, err := s.Commit(ctx, empty); err != nil {
		t.Fatal(err)
	}
	state, err := s.State(ctx)
	if err != nil || state.Sequence != 11 || countRows(t, s, "records") != 10000 {
		t.Fatal("volume loss", err)
	}
	t.Logf("10000 source rows, %d request bytes, %s elapsed; max 1000 rows per transaction", totalBytes, time.Since(start))
}

func TestPostgresSnapshotsStoreOnlyContentTransitions(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	states := []struct {
		payload  string
		deleted  bool
		versions int
	}{
		{`{"amount":"1.00"}`, false, 1},
		{`{"amount":"1.00"}`, false, 1},
		{`{"amount":"2.00"}`, false, 2},
		{`{"amount":"1.00"}`, false, 3},
		{`{"amount":"1.00"}`, true, 4},
		{`{"amount":"1.00"}`, true, 4},
		{`{"amount":"1.00"}`, false, 5},
	}
	for index, state := range states {
		batch := fixtureBatch()
		batch.ID = fmt.Sprintf("snapshot-%d", index)
		batch.ExpectedSequence = int64(index)
		batch.Records[0].Payload = json.RawMessage(state.payload)
		batch.Records[0].Deleted = state.deleted
		receipt, err := s.Commit(ctx, batch)
		if err != nil || receipt.Records != 1 || receipt.Sequence != int64(index+1) {
			t.Fatal("progress lost", receipt, err)
		}
		if countRows(t, s, "records") != 1 || countRows(t, s, "versions") != state.versions || countRows(t, s, "batches") != index+1 {
			t.Fatal("incorrect snapshot history", index)
		}
		var deleted bool
		var payload string
		if err := s.DB.QueryRow(`SELECT payload->>'amount',deleted FROM `+s.name("records")).Scan(&payload, &deleted); err != nil || deleted != state.deleted || payload != strings.TrimSuffix(strings.TrimPrefix(state.payload, `{"amount":"`), `"}`) {
			t.Fatal("wrong current state", payload, deleted, err)
		}
	}
}
