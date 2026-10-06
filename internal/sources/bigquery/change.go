package bigquery

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
)

type changeJob struct {
	Ref    jobRef `json:"jobReference"`
	Status struct {
		State string `json:"state"`
		Error any    `json:"errorResult"`
	} `json:"status"`
	Statistics struct {
		Query struct {
			Affected string `json:"numDmlAffectedRows"`
		} `json:"query"`
	} `json:"statistics"`
}

func (e *Engine) ApplyStatement(parent context.Context, sql string) (_ *int64, err error) {
	if e == nil || parent == nil || len(sql) == 0 || len(sql) > 1<<20 {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid statement")
	}
	client := *e.client
	client.Limit = min(client.Limit, 1<<20)
	ctx, stop := context.WithTimeout(parent, e.limits.Timeout)
	defer stop()
	ref := jobRef{e.source.Options["project"], "kelvo_" + rand.Text(), e.source.Options["location"]}
	base := "/bigquery/v2/projects/" + ref.Project
	q := map[string]any{"query": sql, "useLegacySql": false}
	if v := e.source.Options["dataset"]; v != "" {
		q["defaultDataset"] = map[string]string{"projectId": ref.Project, "datasetId": v}
	}
	if v := e.source.Options["maximum_bytes_billed"]; v != "" {
		q["maximumBytesBilled"] = v
	}
	body := map[string]any{"jobReference": ref, "configuration": map[string]any{"query": q, "jobTimeoutMs": strconv.FormatInt(e.limits.Timeout.Milliseconds(), 10)}}
	defer func() {
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _, _ = client.Do(cleanup, http.MethodPost, base+"/jobs/"+ref.ID+"/cancel?location="+url.QueryEscape(ref.Location), nil, nil, nil)
		}
	}()
	var result changeJob
	_, wire, err := client.Do(ctx, http.MethodPost, base+"/jobs", body, nil, &result)
	if err != nil {
		return nil, err
	}
	for polls := 0; ; polls++ {
		if result.Ref != ref || result.Status.Error != nil {
			return nil, query.NewError("QUERY_FAILED", "Job completion was not acknowledged")
		}
		switch result.Status.State {
		case "DONE":
			if result.Statistics.Query.Affected == "" {
				return nil, nil
			}
			n, parseErr := strconv.ParseInt(result.Statistics.Query.Affected, 10, 64)
			if parseErr != nil || n < 0 {
				// DONE without errorResult acknowledges completion even when the
				// optional count cannot be represented. Never erase that receipt.
				return nil, nil
			}
			return &n, nil
		case "PENDING", "RUNNING":
		default:
			return nil, query.NewError("QUERY_FAILED", "Invalid job state")
		}
		if polls >= 1000 || wire > 8<<20 {
			return nil, query.NewError("RESOURCE_EXHAUSTED", "Job acknowledgement budget exceeded")
		}
		if err = cloudapi.Poll(ctx); err != nil {
			return nil, err
		}
		var next changeJob
		var n int64
		_, n, err = client.Do(ctx, http.MethodGet, base+"/jobs/"+ref.ID+"?location="+url.QueryEscape(ref.Location), nil, nil, &next)
		wire += n
		if err != nil {
			return nil, err
		}
		result = next
	}
}
