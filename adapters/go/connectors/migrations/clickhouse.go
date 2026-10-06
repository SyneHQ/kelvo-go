package migrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/golang-migrate/migrate/v4/database"
)

// ClickHouse callbacks must be bound to one verified HTTP session. Every
// subsequent request uses session_check=1 so load-balancer failover cannot run
// a write on a different node after a successful identity check.
type ClickHouse struct {
	Database string
	Query    func(context.Context, string) ([][]json.RawMessage, error)
	Exec     func(context.Context, string) error
}

func (p ClickHouse) Status(ctx context.Context) (migration.State, error) {
	if ctx == nil || p.Database == "" || p.Query == nil {
		return migration.State{}, migration.ErrInvalid
	}
	d := &clickhouseDriver{ctx: ctx, source: p}
	v, dirty, err := d.Version()
	return migration.State{Version: int64(v), Dirty: dirty}, err
}
func (p ClickHouse) Apply(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	if ctx == nil || p.Database == "" || p.Query == nil || p.Exec == nil || plan.Validate() != nil {
		return initialResult(plan), migration.ErrInvalid
	}
	for _, file := range plan.Files {
		if _, err := clickhouseStatements(file.Content); err != nil {
			return initialResult(plan), err
		}
	}
	d := &clickhouseDriver{ctx: ctx, source: p, plan: &plan}
	defer d.Close()
	return applyPlan(plan, "clickhouse", d, &d.outcome)
}

type clickhouseDriver struct {
	ctx      context.Context
	source   ClickHouse
	plan     *migration.Plan
	owner    string
	locked   bool
	sequence uint64
	outcome
}

var _ database.Driver = (*clickhouseDriver)(nil)

func (*clickhouseDriver) Open(string) (database.Driver, error) { return nil, migration.ErrInvalid }
func (*clickhouseDriver) Drop() error                          { return migration.ErrInvalid }
func (d *clickhouseDriver) Close() error {
	if d.locked && !d.uncertain {
		return d.Unlock()
	}
	return nil
}
func chIdentifier(v string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(v, "\\", "\\\\"), "`", "\\`") + "`"
}
func chString(v string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(v, "\\", "\\\\"), "'", "\\'") + "'"
}
func (d *clickhouseDriver) table(name string) string {
	return chIdentifier(d.source.Database) + "." + chIdentifier(name)
}
func chText(raw json.RawMessage) (string, error) {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", migration.ErrInvalid
	}
	return s, nil
}
func chInt(raw json.RawMessage) (int64, error) {
	var n json.Number
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return 0, migration.ErrInvalid
		}
		n = json.Number(s)
	} else {
		n = json.Number(raw)
	}
	return strconv.ParseInt(string(n), 10, 64)
}
func chUint(raw json.RawMessage) (uint64, error) {
	if len(raw) > 0 && raw[0] == '"' {
		s, err := chText(raw)
		if err != nil {
			return 0, err
		}
		return strconv.ParseUint(s, 10, 64)
	}
	return strconv.ParseUint(string(raw), 10, 64)
}
func (d *clickhouseDriver) object(name string) (string, string, error) {
	rows, err := d.source.Query(d.ctx, "SELECT engine,comment FROM system.tables WHERE database="+chString(d.source.Database)+" AND name="+chString(name)+" LIMIT 2")
	if err != nil {
		return "", "", err
	}
	if len(rows) == 0 {
		return "", "", nil
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		return "", "", migration.ErrInvalid
	}
	engine, err := chText(rows[0][0])
	if err != nil {
		return "", "", err
	}
	comment, err := chText(rows[0][1])
	return engine, comment, err
}
func (d *clickhouseDriver) Version() (int, bool, error) {
	engine, _, err := d.object(migration.Table)
	if err != nil {
		return -1, false, err
	}
	if engine == "" {
		d.sequence = 0
		return -1, false, nil
	}
	if engine != "TinyLog" && engine != "Log" && engine != "StripeLog" {
		return -1, false, migration.ErrInvalid
	}
	rows, err := d.source.Query(d.ctx, "SELECT name,type,default_kind,default_expression FROM system.columns WHERE database="+chString(d.source.Database)+" AND table="+chString(migration.Table)+" ORDER BY position LIMIT 4")
	if err != nil || len(rows) != 3 {
		if err == nil {
			err = migration.ErrInvalid
		}
		return -1, false, err
	}
	for i, want := range [][2]string{{"version", "Int64"}, {"dirty", "UInt8"}, {"sequence", "UInt64"}} {
		if len(rows[i]) != 4 {
			return -1, false, migration.ErrInvalid
		}
		for j := range 4 {
			v, e := chText(rows[i][j])
			if e != nil || j < 2 && v != want[j] || j >= 2 && v != "" {
				return -1, false, migration.ErrInvalid
			}
		}
	}
	rows, err = d.source.Query(d.ctx, "SELECT version,dirty,sequence FROM "+d.table(migration.Table)+" ORDER BY sequence DESC LIMIT 2")
	if err != nil {
		return -1, false, err
	}
	if len(rows) == 0 {
		d.sequence = 0
		return -1, false, nil
	}
	if len(rows[0]) != 3 {
		return -1, false, migration.ErrInvalid
	}
	v, e1 := chInt(rows[0][0])
	dirty, e2 := chUint(rows[0][1])
	sequence, e3 := chUint(rows[0][2])
	if e1 != nil || e2 != nil || e3 != nil || dirty > 1 || (migration.State{Version: v}).Validate() != nil {
		return -1, false, migration.ErrInvalid
	}
	if len(rows) > 1 {
		if len(rows[1]) != 3 {
			return -1, false, migration.ErrInvalid
		}
		previous, e := chUint(rows[1][2])
		if e != nil || previous >= sequence {
			return -1, false, migration.ErrInvalid
		}
	}
	d.sequence = sequence
	return int(v), dirty == 1, nil
}
func (d *clickhouseDriver) Lock() (resultErr error) {
	if d.locked || d.plan == nil {
		return database.ErrLocked
	}
	v, dirty, err := d.Version()
	if err != nil {
		return err
	}
	if (migration.State{Version: int64(v), Dirty: dirty}) != d.plan.Expected {
		return migration.ErrConflict
	}
	if dirty && d.plan.Direction != "force" {
		return migration.ErrDirty
	}
	engine, _, err := d.object(persistentLockTable)
	if err != nil {
		return err
	}
	if engine != "" {
		return database.ErrLocked
	}
	owner := make([]byte, 32)
	if _, err := rand.Read(owner); err != nil {
		return err
	}
	d.owner = "kelvo-owner:" + hex.EncodeToString(owner)
	if err := d.source.Exec(d.ctx, "CREATE TABLE "+d.table(persistentLockTable)+" (lock UInt8) ENGINE=Memory COMMENT "+chString(d.owner)); err != nil {
		d.uncertain = true
		return err
	}
	d.locked = true
	defer func() {
		if resultErr != nil && !d.uncertain {
			_ = d.Unlock()
		}
	}()
	if err := d.requireLock(); err != nil {
		return err
	}
	v, dirty, err = d.Version()
	if err != nil {
		return err
	}
	if (migration.State{Version: int64(v), Dirty: dirty}) != d.plan.Expected {
		return migration.ErrConflict
	}
	if dirty && d.plan.Direction != "force" {
		return migration.ErrDirty
	}
	engine, _, err = d.object(migration.Table)
	if err != nil {
		return err
	}
	if engine == "" {
		if err = d.source.Exec(d.ctx, "CREATE TABLE "+d.table(migration.Table)+" (version Int64, dirty UInt8, sequence UInt64) ENGINE=TinyLog"); err != nil {
			d.uncertain = true
			return err
		}
		d.changed = true
	}
	return nil
}
func (d *clickhouseDriver) requireLock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	engine, owner, err := d.object(persistentLockTable)
	if err != nil || engine != "Memory" || owner != d.owner {
		d.uncertain = true
		return migration.ErrOutcomeUnknown
	}
	return nil
}
func (d *clickhouseDriver) Unlock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	if d.uncertain {
		return migration.ErrOutcomeUnknown
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	v, dirty, err := d.Version()
	if err != nil {
		d.uncertain = true
		return err
	}
	if err = d.source.Exec(d.ctx, "DROP TABLE "+d.table(persistentLockTable)+" SYNC"); err != nil {
		d.uncertain = true
		return err
	}
	d.locked = false
	d.finalRead = true
	d.finalState = migration.State{Version: int64(v), Dirty: dirty}
	return nil
}
func (d *clickhouseDriver) SetVersion(v int, dirty bool) error {
	if (migration.State{Version: int64(v)}).Validate() != nil {
		return migration.ErrInvalid
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	if _, _, err := d.Version(); err != nil {
		return err
	}
	if d.sequence == math.MaxUint64 {
		return migration.ErrInvalid
	}
	flag := 0
	if dirty {
		flag = 1
	}
	sequence := d.sequence + 1
	if err := d.source.Exec(d.ctx, "INSERT INTO "+d.table(migration.Table)+" (version,dirty,sequence) VALUES ("+strconv.Itoa(v)+","+strconv.Itoa(flag)+","+strconv.FormatUint(sequence, 10)+")"); err != nil {
		d.uncertain = true
		return err
	}
	d.sequence = sequence
	d.changed = true
	return nil
}
func (d *clickhouseDriver) Run(input io.Reader) error {
	raw, err := io.ReadAll(io.LimitReader(input, migration.MaxFileBytes+1))
	if err != nil || len(raw) > migration.MaxFileBytes {
		return migration.ErrInvalid
	}
	defer clear(raw)
	statements, err := clickhouseStatements(string(raw))
	if err != nil {
		return err
	}
	for _, sql := range statements {
		if err := d.requireLock(); err != nil {
			return err
		}
		if err := d.source.Exec(d.ctx, sql); err != nil {
			d.uncertain = true
			return err
		}
		d.changed = true
		if err := d.requireLock(); err != nil {
			return err
		}
	}
	return nil
}

// Split only outside quoted values/comments. Each statement stays byte exact.
// Keep cluster/remote engines, session controls and history edits outside this
// deliberately single-node migration contract.
func clickhouseStatements(raw string) ([]string, error) {
	var result []string
	var code strings.Builder
	start := 0
	var quote byte
	finish := func(end int) error {
		statement := strings.TrimSpace(raw[start:end])
		tokens := strings.Fields(strings.ToLower(code.String()))
		code.Reset()
		start = end + 1
		if len(tokens) == 0 {
			return nil
		}
		switch tokens[0] {
		case "create", "alter", "drop", "rename", "truncate", "insert", "update", "delete", "select":
		default:
			return migration.ErrInvalid
		}
		for _, token := range tokens {
			switch token {
			case "cluster", "distributed", "replicated", "shared", "remote", "remotesecure", "url", "s3", "s3cluster", "azureblobstorage", "hdfs", "mysql", "postgresql", "odbc", "jdbc", "settings", "format", "outfile", "schema_migrations", "kelvo_migration_lock":
				return migration.ErrInvalid
			}
			if strings.HasPrefix(token, "replicated") || strings.HasPrefix(token, "shared") {
				return migration.ErrInvalid
			}
		}
		result = append(result, statement)
		if len(result) > 64 {
			return migration.ErrInvalid
		}
		return nil
	}
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if quote != 0 {
			if b == '\\' {
				if quote != '\'' {
					return nil, migration.ErrInvalid
				}
				i++
				continue
			}
			if b == quote {
				if i+1 < len(raw) && raw[i+1] == quote {
					i++
					continue
				}
				quote = 0
				code.WriteByte(' ')
			} else if quote != '\'' {
				code.WriteByte(b)
			}
			continue
		}
		if b == '\'' || b == '"' || b == '`' {
			quote = b
			code.WriteByte(' ')
			continue
		}
		if b == '-' && i+1 < len(raw) && raw[i+1] == '-' || b == '#' {
			for i < len(raw) && raw[i] != '\n' {
				i++
			}
			code.WriteByte(' ')
			continue
		}
		if b == '/' && i+1 < len(raw) && raw[i+1] == '*' {
			end := strings.Index(raw[i+2:], "*/")
			if end < 0 || strings.Contains(raw[i+2:i+2+end], "/*") {
				return nil, migration.ErrInvalid
			}
			i += end + 3
			code.WriteByte(' ')
			continue
		}
		if b == ';' {
			if err := finish(i); err != nil {
				return nil, err
			}
			continue
		}
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' {
			code.WriteByte(b)
		} else {
			code.WriteByte(' ')
		}
	}
	if quote != 0 {
		return nil, migration.ErrInvalid
	}
	if err := finish(len(raw)); err != nil {
		return nil, err
	}
	return result, nil
}
