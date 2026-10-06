// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/xwb1989/sqlparser"
)

type projection struct{ field, alias, aggregate string }
type predicate struct {
	field, op string
	values    []any
}
type ordering struct {
	field string
	desc  bool
}
type selectPlan struct {
	table   string
	columns []projection
	filters []predicate
	having  []predicate
	groups  []string
	orders  []ordering
	limit   int64
	offset  int64
	limited bool
	star    bool
}

var fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_:]{0,127}$`)
var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func column(expr sqlparser.Expr) (string, error) {
	c, ok := expr.(*sqlparser.ColName)
	if !ok || !c.Qualifier.IsEmpty() || !fieldName.MatchString(c.Name.String()) {
		return "", adapter.ErrUnsupported
	}
	return c.Name.String(), nil
}
func literal(expr sqlparser.Expr) (any, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return string(v.Val), nil
		case sqlparser.IntVal, sqlparser.FloatVal:
			if jsonNumber.Match(v.Val) {
				return json.Number(v.Val), nil
			}
		}
	case sqlparser.BoolVal:
		return bool(v), nil
	case *sqlparser.UnaryExpr:
		if v.Operator == "-" {
			value, err := literal(v.Expr)
			if n, ok := value.(json.Number); err == nil && ok && !strings.HasPrefix(string(n), "-") {
				return json.Number("-" + n), nil
			}
		}
	}
	return nil, adapter.ErrUnsupported
}
func predicates(expr sqlparser.Expr) ([]predicate, error) {
	if expr == nil {
		return nil, nil
	}
	switch v := expr.(type) {
	case *sqlparser.ParenExpr:
		return predicates(v.Expr)
	case *sqlparser.AndExpr:
		left, err := predicates(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := predicates(v.Right)
		if len(left)+len(right) > 32 {
			return nil, adapter.ErrLimit
		}
		return append(left, right...), err
	case *sqlparser.ComparisonExpr:
		name, err := column(v.Left)
		if err != nil || v.Escape != nil {
			return nil, adapter.ErrUnsupported
		}
		p := predicate{field: name, op: v.Operator}
		if v.Operator == "in" {
			values, ok := v.Right.(sqlparser.ValTuple)
			if !ok || len(values) == 0 || len(values) > 100 {
				return nil, adapter.ErrUnsupported
			}
			for _, value := range values {
				x, err := literal(value)
				if err != nil {
					return nil, err
				}
				p.values = append(p.values, x)
			}
		} else {
			switch v.Operator {
			case "=", "!=", "<", "<=", ">", ">=":
			default:
				return nil, adapter.ErrUnsupported
			}
			x, err := literal(v.Right)
			if err != nil {
				return nil, err
			}
			p.values = []any{x}
		}
		return []predicate{p}, nil
	case *sqlparser.RangeCond:
		name, err := column(v.Left)
		if err != nil || v.Operator != "between" {
			return nil, adapter.ErrUnsupported
		}
		from, err := literal(v.From)
		if err != nil {
			return nil, err
		}
		to, err := literal(v.To)
		if err != nil {
			return nil, err
		}
		return []predicate{{field: name, op: "between", values: []any{from, to}}}, nil
	case *sqlparser.IsExpr:
		name, err := column(v.Expr)
		if err != nil || v.Operator != "is null" && v.Operator != "is not null" {
			return nil, adapter.ErrUnsupported
		}
		return []predicate{{field: name, op: v.Operator}}, nil
	}
	return nil, adapter.ErrUnsupported
}

func parseSelect(query string) (selectPlan, error) {
	p := selectPlan{}
	stmt, err := sqlparser.Parse(query)
	q, ok := stmt.(*sqlparser.Select)
	if err != nil || !ok || q.Distinct != "" || q.Hints != "" || q.Cache != "" || q.Lock != "" || len(q.From) != 1 || len(q.SelectExprs) == 0 || len(q.SelectExprs) > 64 {
		return p, adapter.ErrUnsupported
	}
	table, ok := q.From[0].(*sqlparser.AliasedTableExpr)
	if !ok || !table.As.IsEmpty() || table.Hints != nil || len(table.Partitions) != 0 {
		return p, adapter.ErrUnsupported
	}
	name, ok := table.Expr.(sqlparser.TableName)
	if !ok || !name.Qualifier.IsEmpty() || !fieldName.MatchString(name.Name.String()) {
		return p, adapter.ErrUnsupported
	}
	p.table = strings.ToLower(name.Name.String())
	aliases := map[string]bool{}
	for _, expr := range q.SelectExprs {
		if star, ok := expr.(*sqlparser.StarExpr); ok {
			if !star.TableName.IsEmpty() || len(q.SelectExprs) != 1 {
				return p, adapter.ErrUnsupported
			}
			p.star = true
			continue
		}
		item, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return p, adapter.ErrUnsupported
		}
		c := projection{alias: item.As.String()}
		if f, ok := item.Expr.(*sqlparser.FuncExpr); ok {
			c.aggregate = strings.ToLower(f.Name.String())
			if !f.Qualifier.IsEmpty() || f.Distinct || len(f.Exprs) != 1 {
				return p, adapter.ErrUnsupported
			}
			switch c.aggregate {
			case "count", "sum", "min", "max":
			default:
				return p, adapter.ErrUnsupported
			}
			if star, ok := f.Exprs[0].(*sqlparser.StarExpr); ok && star.TableName.IsEmpty() && c.aggregate == "count" {
				c.field = "*"
			} else if value, ok := f.Exprs[0].(*sqlparser.AliasedExpr); ok && value.As.IsEmpty() {
				c.field, err = column(value.Expr)
			} else {
				err = adapter.ErrUnsupported
			}
			if c.alias == "" {
				c.alias = c.aggregate + "(" + c.field + ")"
			}
		} else {
			c.field, err = column(item.Expr)
			if c.alias == "" {
				c.alias = c.field
			}
		}
		if err != nil || c.alias == "" || aliases[c.alias] {
			return p, adapter.ErrUnsupported
		}
		aliases[c.alias] = true
		p.columns = append(p.columns, c)
	}
	if q.Where != nil {
		p.filters, err = predicates(q.Where.Expr)
		if err != nil {
			return p, err
		}
	}
	if q.Having != nil {
		p.having, err = predicates(q.Having.Expr)
		if err != nil {
			return p, err
		}
	}
	for _, expr := range q.GroupBy {
		name, err := column(expr)
		if err != nil {
			return p, err
		}
		p.groups = append(p.groups, name)
	}
	for _, order := range q.OrderBy {
		name, err := column(order.Expr)
		if err != nil || order.Direction != sqlparser.AscScr && order.Direction != sqlparser.DescScr {
			return p, adapter.ErrUnsupported
		}
		p.orders = append(p.orders, ordering{field: name, desc: order.Direction == sqlparser.DescScr})
	}
	if q.Limit != nil {
		p.limited = true
		p.limit, err = positiveInteger(q.Limit.Rowcount)
		if err != nil {
			return p, err
		}
		if q.Limit.Offset != nil {
			p.offset, err = positiveInteger(q.Limit.Offset)
			if err != nil {
				return p, err
			}
		}
	}
	return p, nil
}
func positiveInteger(expr sqlparser.Expr) (int64, error) {
	v, ok := expr.(*sqlparser.SQLVal)
	if !ok || v.Type != sqlparser.IntVal {
		return 0, adapter.ErrUnsupported
	}
	n, err := strconv.ParseInt(string(v.Val), 10, 64)
	if err != nil || n < 0 || n > maxRows {
		return 0, adapter.ErrLimit
	}
	return n, nil
}
