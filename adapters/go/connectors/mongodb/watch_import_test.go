package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/watch"
)

func testImport(scope watch.Scope) watch.Import {
	return watch.Import{Version: watch.Version, ScopeSHA256: scope.Key(), Generation: scope.Generation, SourceIdentitySHA256: strings.Repeat("a", 64), SourceRevision: "revision-a", ResumeToken: json.RawMessage(`{"_data":"legacy-position"}`)}
}

func TestMongoImportStartsAtExactLegacyTokenAndNeverResetsActiveCursor(t *testing.T) {
	scope := mongoWatchScope()
	resume := testImport(scope)
	backend := &fakeWatchBackend{cursor: &fakeWatchCursor{initial: testToken("newer-post-batch-token")}}
	worker := mongoWatch{scope, backend}
	if err := worker.importLegacy(context.Background(), resume); err != nil {
		t.Fatal(err)
	}
	if backend.creates != 1 || backend.state.Token != tokenText("legacy-position") || backend.state.ImportSHA256 == "" {
		t.Fatal("legacy position changed")
	}
	original := backend.state.ImportSHA256
	backend.state.Token = tokenText("advanced-by-ack")
	if err := worker.importLegacy(context.Background(), resume); err != nil || backend.opens != 1 || backend.state.Token != tokenText("advanced-by-ack") || backend.state.ImportSHA256 != original {
		t.Fatal("idempotent import reset acknowledged progress", err)
	}
	changed := resume
	changed.ResumeToken = json.RawMessage(`{"_data":"different"}`)
	if err := worker.importLegacy(context.Background(), changed); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("different import overwrote active cursor", err)
	}
	if err := worker.remove(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.importLegacy(context.Background(), resume); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("retired import resurrected", err)
	}
}

func TestMongoImportNeverReplacesLiveInstallationOrInvalidToken(t *testing.T) {
	backend, worker := activeFake()
	resume := testImport(worker.scope)
	if err := worker.importLegacy(context.Background(), resume); !errors.Is(err, watch.ErrConflict) || backend.opens != 0 || backend.writes != 0 {
		t.Fatal("ordinary active cursor was overwritten", err)
	}
	backend = &fakeWatchBackend{openErr: errors.New("expired token")}
	worker = mongoWatch{mongoWatchScope(), backend}
	if err := worker.importLegacy(context.Background(), resume); err == nil || backend.creates != 0 {
		t.Fatal("expired token reset to current position")
	}
	resume.ResumeToken = json.RawMessage(`{"$where":"evil"}`)
	if err := worker.importLegacy(context.Background(), resume); err == nil || backend.opens != 1 {
		t.Fatal("non-token accepted")
	}
}
