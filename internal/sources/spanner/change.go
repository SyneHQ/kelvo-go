package spanner

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
)

var changeStart = regexp.MustCompile(`(?is)^(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*(insert|update|delete|create|alter|drop)\b`)

func (e *Engine) ValidateStatement(sql string) error {
	if len(sql) == 0 || len(sql) > 1<<20 || !changeStart.MatchString(sql) {
		return query.NewError("UNSUPPORTED", "Spanner requires one DML or schema statement")
	}
	return nil
}

func (e *Engine) ApplyStatement(parent context.Context, sql string) (_ *int64, err error) {
	if e == nil || parent == nil {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid statement")
	}
	if err = e.ValidateStatement(sql); err != nil {
		return nil, err
	}
	verb := strings.ToLower(changeStart.FindStringSubmatch(sql)[1])
	if verb == "create" || verb == "alter" || verb == "drop" {
		return e.applyDDL(parent, sql)
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	client := *e.client
	client.Limit = min(client.Limit, 1<<20)
	var session struct {
		Name string `json:"name"`
	}
	_, _, err = client.Do(ctx, http.MethodPost, "/v1/"+e.database+"/sessions", map[string]any{"session": map[string]any{}}, nil, &session)
	if err != nil {
		return nil, err
	}
	prefix := e.database + "/sessions/"
	if !strings.HasPrefix(session.Name, prefix) || !sessionID.MatchString(strings.TrimPrefix(session.Name, prefix)) {
		return nil, query.NewError("QUERY_FAILED", "Invalid Spanner session")
	}
	path := "/v1/" + session.Name
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_, _, _ = client.Do(cleanup, http.MethodDelete, path, nil, nil, nil)
	}()
	var transaction struct {
		ID string `json:"id"`
	}
	_, _, err = client.Do(ctx, http.MethodPost, path+":beginTransaction", map[string]any{"options": map[string]any{"readWrite": map[string]any{}}}, nil, &transaction)
	if err != nil {
		return nil, err
	}
	tx, decodeErr := base64.StdEncoding.Strict().DecodeString(transaction.ID)
	if decodeErr != nil || len(tx) == 0 || len(tx) > 4096 {
		return nil, query.NewError("QUERY_FAILED", "Invalid Spanner transaction")
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = client.Do(cleanup, http.MethodPost, path+":rollback", map[string]string{"transactionId": transaction.ID}, nil, nil)
		}
	}()
	var result struct {
		Stats struct {
			Count string `json:"rowCountExact"`
		} `json:"stats"`
	}
	_, _, err = client.Do(ctx, http.MethodPost, path+":executeSql", map[string]any{"sql": sql, "transaction": map[string]string{"id": transaction.ID}, "seqno": "1", "queryMode": "NORMAL"}, nil, &result)
	if err != nil {
		return nil, err
	}
	var ack struct {
		Timestamp string `json:"commitTimestamp"`
	}
	_, _, err = client.Do(ctx, http.MethodPost, path+":commit", map[string]string{"transactionId": transaction.ID}, nil, &ack)
	if err != nil {
		return nil, err
	}
	if _, err = time.Parse(time.RFC3339Nano, ack.Timestamp); err != nil {
		return nil, query.NewError("QUERY_FAILED", "Spanner commit was not acknowledged")
	}
	committed = true
	if result.Stats.Count != "" {
		if count, err := strconv.ParseInt(result.Stats.Count, 10, 64); err == nil && count >= 0 {
			return &count, nil
		}
	}
	return nil, nil
}

type changeOperation struct {
	Name  string `json:"name"`
	Done  bool   `json:"done"`
	Error any    `json:"error"`
}

func (e *Engine) applyDDL(parent context.Context, sql string) (_ *int64, err error) {
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	client := *e.client
	client.Limit = min(client.Limit, 1<<20)
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return nil, err
	}
	id := "kelvo_" + hex.EncodeToString(random[:])
	name := e.database + "/operations/" + id
	var result changeOperation
	_, wire, err := client.Do(ctx, http.MethodPatch, "/v1/"+e.database+"/ddl", map[string]any{"statements": []string{sql}, "operationId": id}, nil, &result)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = client.Do(cleanup, http.MethodPost, "/v1/"+name+":cancel", map[string]any{}, nil, nil)
		}
	}()
	for polls := 0; ; polls++ {
		if result.Name != name || result.Error != nil {
			return nil, query.NewError("QUERY_FAILED", "Spanner schema operation was not acknowledged")
		}
		if result.Done {
			return nil, nil
		}
		if polls >= 1000 || wire > 8<<20 {
			return nil, query.NewError("RESOURCE_EXHAUSTED", "Schema acknowledgement budget exceeded")
		}
		if err = cloudapi.Poll(ctx); err != nil {
			return nil, err
		}
		var next changeOperation
		var n int64
		_, n, err = client.Do(ctx, http.MethodGet, "/v1/"+name, nil, nil, &next)
		wire += n
		if err != nil {
			return nil, err
		}
		result = next
	}
}
