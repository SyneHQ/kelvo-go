// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/xwb1989/sqlparser"
)

// Routing reasons reported in query.AccelerationVersion.RoutedLive.
const (
	RouteBeyondCovered = "beyond_covered"
	RouteUnbounded     = "unbounded"
	RouteUnparsed      = "unparsed"
)

// RouteReason decides whether a query on dataset alias needs rows newer than
// coveredUntil (MAX of timeColumn at refresh: a decimal integer or RFC3339Nano
// UTC time). It returns "" when the snapshot suffices, otherwise the reason to
// read live. Only simple top-level WHERE conjuncts on the column are read; any
// other conjunct on the column, or any shape it cannot prove, reads live because
// the snapshot may be stale. ponytail: the parser is MySQL-dialect, so
// DuckDB-only syntax is "unparsed" (live); swap in a DuckDB parse if that bites.
func RouteReason(sql, alias, timeColumn, coveredUntil string) string {
	stmt, err := sqlparser.Parse(sql)
	sel, ok := stmt.(*sqlparser.Select)
	if err != nil || !ok || len(sel.From) != 1 {
		return RouteUnparsed
	}
	table, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return RouteUnparsed
	}
	name, ok := table.Expr.(sqlparser.TableName)
	if !ok || !name.Qualifier.IsEmpty() || name.Name.String() != alias {
		return RouteUnparsed
	}
	qualifier := alias
	if !table.As.IsEmpty() {
		qualifier = table.As.String()
	}
	if sel.Where == nil {
		return RouteUnbounded
	}
	reason, confusing := RouteUnbounded, false
	for _, conjunct := range conjuncts(sel.Where.Expr) {
		if !mentions(conjunct, timeColumn) {
			continue // another predicate only narrows the rows further
		}
		upper, understood := upperBound(conjunct, qualifier, timeColumn)
		if !understood {
			confusing = true
			continue
		}
		if upper == nil {
			continue // a lower bound only
		}
		within, comparable := atMost(upper, coveredUntil)
		switch {
		case !comparable:
			confusing = true
		case within:
			return ""
		default:
			reason = RouteBeyondCovered
		}
	}
	if confusing {
		return RouteUnparsed
	}
	return reason
}

func conjuncts(e sqlparser.Expr) []sqlparser.Expr {
	switch e := e.(type) {
	case *sqlparser.AndExpr:
		return append(conjuncts(e.Left), conjuncts(e.Right)...)
	case *sqlparser.ParenExpr:
		return conjuncts(e.Expr)
	}
	return []sqlparser.Expr{e}
}

func mentions(e sqlparser.Expr, column string) bool {
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if c, ok := n.(*sqlparser.ColName); ok && strings.EqualFold(c.Name.String(), column) {
			found = true
		}
		return !found, nil
	}, e)
	return found
}

func isColumn(e sqlparser.Expr, qualifier, column string) bool {
	c, ok := e.(*sqlparser.ColName)
	return ok && strings.EqualFold(c.Name.String(), column) && c.Qualifier.Qualifier.IsEmpty() &&
		(c.Qualifier.IsEmpty() || c.Qualifier.Name.String() == qualifier)
}

func literal(e sqlparser.Expr) (*sqlparser.SQLVal, bool) {
	v, ok := e.(*sqlparser.SQLVal)
	return v, ok && (v.Type == sqlparser.IntVal || v.Type == sqlparser.StrVal)
}

// upperBound returns the literal the column is bounded above by, nil for a
// lower bound, and false when the conjunct is not one of the understood forms.
func upperBound(e sqlparser.Expr, qualifier, column string) (*sqlparser.SQLVal, bool) {
	switch e := e.(type) {
	case *sqlparser.ComparisonExpr:
		op, left, right := e.Operator, e.Left, e.Right
		if isColumn(right, qualifier, column) { // lit op col
			left, right = right, left
			op = map[string]string{"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}[op]
		}
		v, ok := literal(right)
		if !isColumn(left, qualifier, column) || !ok {
			return nil, false
		}
		switch op {
		case "<", "<=", "=":
			return v, true
		case ">", ">=":
			return nil, true
		}
	case *sqlparser.RangeCond:
		to, ok := literal(e.To)
		_, from := literal(e.From)
		if e.Operator == sqlparser.BetweenStr && isColumn(e.Left, qualifier, column) && ok && from {
			return to, true
		}
	}
	return nil, false
}

// atMost compares an SQL literal with covered_until; comparable is false when
// the literal's type or format is not understood.
func atMost(v *sqlparser.SQLVal, covered string) (within, comparable bool) {
	if n, err := strconv.ParseInt(covered, 10, 64); err == nil {
		lit, err := strconv.ParseInt(string(v.Val), 10, 64)
		return lit <= n, v.Type == sqlparser.IntVal && err == nil
	}
	until, err := time.Parse(time.RFC3339Nano, covered)
	if err != nil || v.Type != sqlparser.StrVal {
		return false, false
	}
	for _, layout := range []string{"2006-01-02", "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", time.RFC3339Nano} {
		if t, err := time.Parse(layout, string(v.Val)); err == nil {
			return !t.After(until), true
		}
	}
	return false, false
}

// Router plans live routes for datasets that configure route. It needs only the
// node's snapshot metadata reader; without one every dataset reads its snapshot.
type Router struct {
	Status func(context.Context, string) (Snapshot, error)
}

// Plan returns request with each dataset that must read live replaced by its
// live source, and the reason per dataset. ok is false when nothing routes or
// the substitution is not a valid selection (for example the live source is
// already requested); the caller then reads snapshots. The caller must still
// authorize the substituted request exactly as a direct read of those sources.
func (rt *Router) Plan(ctx context.Context, c catalog.Config, request query.Request) (live query.Request, reasons map[string]string, ok bool) {
	if request.Mode == "native" {
		return request, nil, false
	}
	live = request
	live.Sources = append([]string(nil), request.Sources...)
	for i, id := range request.Sources {
		d, configured := c.Dataset(id)
		if !configured || d.Route == nil {
			continue
		}
		fingerprint, err := c.DatasetFingerprint(id)
		snapshot, statusErr := rt.Status(ctx, id)
		if err != nil || statusErr != nil || snapshot.Fingerprint != fingerprint || snapshot.CoveredUntil == "" {
			continue // missing covered_until never routes
		}
		reason := RouteReason(request.SQL, id, d.Route.TimeColumn, routeCovered(d, snapshot.CoveredUntil))
		if reason == "" {
			continue
		}
		if reasons == nil {
			reasons = map[string]string{}
		}
		reasons[id], live.Sources[i] = reason, d.Route.LiveSource
	}
	if len(reasons) == 0 {
		return request, nil, false
	}
	if _, err := c.Select(live.Sources); err != nil {
		return request, nil, false
	}
	return live, reasons, true
}

// routeCovered is the bound the snapshot is trusted up to. An incremental
// dataset with late_window may still receive rows down to covered_until minus
// that window, so reads reaching past it go live.
func routeCovered(d catalog.Dataset, covered string) string {
	if d.Incremental == nil || d.Incremental.LateWindow == 0 {
		return covered
	}
	t, err := time.Parse(time.RFC3339Nano, covered)
	if err != nil {
		return covered // integer watermarks reject late_window at refresh
	}
	return t.Add(-d.Incremental.LateWindow).UTC().Format(time.RFC3339Nano)
}
