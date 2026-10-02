#!/usr/bin/env python3
"""Exercise the documented public SDK and a compiled-in adapter on a Linux VM.

prepare extracts the documentation verbatim into an external module and checks
its public import boundary. build copies the source into isolated scratch,
adds a tagged registration import and builds one adapter-enabled CLI. run checks
real CLI/re-executed-worker results and failures. No source checkout is edited.
The four-row companion driver is an acceptance fixture, not a database adapter.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid


MODULE = "example.org/kelvo-federation-acceptance"
KELVO = "github.com/SYNEHQ/kelvo-go"
TAG = "adapter_acceptance"

FIXTURE = r'''package fixtureadapter

import (
    "context"
    "os"
    "strconv"
    "github.com/SYNEHQ/kelvo-go/federation"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func init() { federation.MustRegister("acceptance_rows", driver{}) }
type driver struct{}
func (driver) Validate(_ federation.Source, t federation.Table) error {
    if t.Database != "" || t.Schema != "" || t.Table != "rows" { return federation.ErrUnsupported }
    return nil
}
func (d driver) Open(ctx context.Context, s federation.Source, t federation.Table, _ federation.Limits) (federation.Relation, error) {
    if err := ctx.Err(); err != nil { return nil, err }
    if err := d.Validate(s,t); err != nil { return nil,err }
    return &relation{arrow.NewSchema([]arrow.Field{
        {Name:"id", Type:arrow.PrimitiveTypes.Int64},
        {Name:"label", Type:arrow.BinaryTypes.String, Nullable:true},
        {Name:"worker_pid", Type:arrow.PrimitiveTypes.Int64},
    },nil)},nil
}
type relation struct { schema *arrow.Schema }
func (r *relation) Schema() *arrow.Schema { return r.schema }
func (r *relation) Close() error { return nil }
func matches(f federation.Filter, id int64) (bool,error) {
    switch f.Kind {
    case "and", "or":
        if len(f.Children)==0 { return false,federation.ErrUnsupported }
        result := f.Kind=="and"
        for _, child := range f.Children {
            value,err := matches(child,id); if err!=nil { return false,err }
            if f.Kind=="and" { result=result&&value } else { result=result||value }
        }
        return result,nil
    case "is_null", "is_not_null":
        if f.Column!="id" { return false,federation.ErrUnsupported }
        return f.Kind=="is_not_null",nil
    case "comparison":
        if f.Column!="id" || f.Type!="int64" { return false,federation.ErrUnsupported }
        value,err := strconv.ParseInt(f.Value,10,64)
        if err!=nil || strconv.FormatInt(value,10)!=f.Value { return false,federation.ErrUnsupported }
        switch f.Op {
        case "eq": return id==value,nil
        case "ne": return id!=value,nil
        case "lt": return id<value,nil
        case "le": return id<=value,nil
        case "gt": return id>value,nil
        case "ge": return id>=value,nil
        }
    }
    return false,federation.ErrUnsupported
}
func (r *relation) Scan(ctx context.Context, plan federation.ScanPlan, sink federation.Sink) (federation.ScanStats,error) {
    stats := federation.ScanStats{}
    fields := make([]arrow.Field,len(plan.Columns))
    for i,name := range plan.Columns {
        found:=false
        for _,field:=range r.schema.Fields() { if field.Name==name { fields[i],found=field,true; break } }
        if !found { return stats,federation.ErrUnsupported }
    }
    schema:=arrow.NewSchema(fields,nil)
    if err:=sink.Schema(schema); err!=nil { return stats,err }
    builder:=array.NewRecordBuilder(memory.DefaultAllocator,schema); defer builder.Release()
    // This fixed-size fixture intentionally emits its complete relation so
    // acceptance can verify the core's independent scan-limit enforcement.
    for id:=int64(1);id<=4;id++ {
        if err:=ctx.Err();err!=nil { return stats,err }
        keep:=true
        for _,filter:=range plan.Filters {
            value,err:=matches(filter,id);if err!=nil{return stats,err};keep=keep&&value
        }
        if !keep {continue}
        for i,name:=range plan.Columns {
            switch name {
            case "id": builder.Field(i).(*array.Int64Builder).Append(id)
            case "worker_pid": builder.Field(i).(*array.Int64Builder).Append(int64(os.Getpid()))
            case "label":
                b:=builder.Field(i).(*array.StringBuilder)
                if id==4 { b.AppendNull() } else if id==2 { b.Append("drop") } else { b.Append("keep") }
            }
        }
    }
    record:=builder.NewRecordBatch();defer record.Release()
    if record.NumRows()==0 {return stats,nil}
    return stats,sink.Write(record)
}
'''

EXAMPLE_TEST = r'''package staticadapter_test
import (
    "context"
    "errors"
    "testing"
    _ "example.org/kelvo-federation-acceptance/staticadapter"
    "github.com/SYNEHQ/kelvo-go/federation"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
)
type capture struct { schema *arrow.Schema; values []int64 }
func (s *capture) Schema(v *arrow.Schema) error {s.schema=v;return nil}
func (s *capture) Write(r arrow.RecordBatch) error {
    a:=r.Column(0).(*array.Int64);for i:=0;i<a.Len();i++ {s.values=append(s.values,a.Value(i))};return nil
}
func TestVerbatimDocumentationExample(t *testing.T) {
    driver,ok:=federation.Lookup("example_static");if !ok {t.Fatal("example did not register")}
    relation,err:=driver.Open(context.Background(),federation.Source{ID:"demo",Type:"example_static"},federation.Table{Name:"sample",Table:"one_row"},federation.Limits{})
    if err!=nil {t.Fatal(err)};defer relation.Close()
    sink:=&capture{}
    if _,err=relation.Scan(context.Background(),federation.ScanPlan{Columns:[]string{"id"}},sink);err!=nil {t.Fatal(err)}
    if sink.schema.NumFields()!=1 || len(sink.values)!=1 || sink.values[0]!=1 {t.Fatal("documented row changed")}
    _,err=relation.Scan(context.Background(),federation.ScanPlan{Columns:[]string{"id"},Filters:[]federation.Filter{{Kind:"comparison",Column:"id",Type:"int64",Op:"eq",Value:"1"}}},sink)
    if !errors.Is(err,federation.ErrUnsupported) {t.Fatal("example silently accepted unsupported filter")}
}
'''

DECODER = r'''package main
import (
    "encoding/json"
    "fmt"
    "os"
    "strings"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/ipc"
)
func main() {
    file,err:=os.Open(os.Args[1]);if err!=nil {panic(err)};defer file.Close()
    reader,err:=ipc.NewReader(file);if err!=nil {panic(err)};defer reader.Release()
    fields:=[]string{};for _,field:=range reader.Schema().Fields(){fields=append(fields,field.Name)}
    rows:=[][]any{}
    for reader.Next(){record:=reader.RecordBatch();for i:=0;i<int(record.NumRows());i++{
        row:=[]any{};for _,column:=range record.Columns(){
            if column.IsNull(i){row=append(row,nil);continue}
            switch a:=column.(type){
            case *array.Int64:row=append(row,a.Value(i))
            case *array.String:row=append(row,strings.Clone(a.Value(i)))
            default:panic(fmt.Sprintf("unexpected output type %s",column.DataType()))
            }
        };rows=append(rows,row)
    }}
    if err:=reader.Err();err!=nil{panic(err)}
    if err:=json.NewEncoder(os.Stdout).Encode(map[string]any{"fields":fields,"rows":rows});err!=nil{panic(err)}
}
'''


def require(value, message):
    if not value:
        raise AssertionError(message)


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            result.update(block)
    return result.hexdigest()


def command(argv, directory, environment, log, timeout=120):
    started = time.monotonic()
    result = subprocess.run(argv, cwd=directory, env=environment, capture_output=True,
                            text=True, timeout=timeout)
    write(log, result.stdout + result.stderr)
    require(result.returncode == 0, f"command failed; inspect {log}")
    return {"argv": argv, "seconds": round(time.monotonic() - started, 3),
            "log": str(log), "stdout": result.stdout}


def environment():
    value = dict(os.environ)
    value.update(GOMAXPROCS="2", GOMEMLIMIT="1GiB", GOPROXY="off", GOSUMDB="off", GOWORK="off")
    return value


def prepare(args):
    directory, source = args.directory, args.source
    marker = directory / "manifest.json"
    require(not marker.exists(), "prepare requires a fresh acceptance directory")
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    blocks = re.findall(r"^```go\s*\n(.*?)^```", (source / "docs/federation-adapters.md").read_text(), re.M | re.S)
    examples = [block for block in blocks if block.startswith("package staticadapter\n")]
    require(len(examples) == 1, "documentation must contain one complete staticadapter package")
    example = examples[0]
    imports = re.findall(r'"(github\.com/SYNEHQ/kelvo-go/[^\"]+)"', example + FIXTURE)
    require(imports and set(imports) == {KELVO + "/federation"}, "adapter imports must use only the public SDK")
    module = directory / "external-module"
    version = re.search(r"^go (\S+)", (source / "go.mod").read_text(), re.M).group(1)
    write(module / "go.mod", f"module {MODULE}\n\ngo {version}\n\nrequire {KELVO} v0.0.0\nreplace {KELVO} => {source}\n")
    shutil.copyfile(source / "go.sum", module / "go.sum")
    write(module / "staticadapter/adapter.go", example)
    write(module / "staticadapter/adapter_test.go", EXAMPLE_TEST)
    write(module / "fixtureadapter/adapter.go", FIXTURE)
    write(module / "cmd/decode/main.go", DECODER)
    env = environment()
    tested = command([args.go, "test", "-p", "2", "-mod=mod", "./staticadapter", "./fixtureadapter"], module, env, directory / "external-test.log")
    dependencies = command([args.go, "list", "-mod=mod", "-deps", "./staticadapter", "./fixtureadapter"], module, env, directory / "public-dependencies.log")
    require(KELVO + "/internal/" not in dependencies["stdout"], "public adapter reached Kelvo internals")
    command([args.go, "build", "-p", "2", "-mod=mod", "-o", str(directory / "decode-arrow"), "./cmd/decode"], module, env, directory / "decoder-build.log")
    manifest = {"source": str(source), "external_module": str(module),
                "example_sha256": hashlib.sha256(example.encode()).hexdigest(),
                "repository_imports": sorted(set(imports)), "external_test": tested,
                "scope": "Verbatim teaching example plus a separate four-row public-SDK acceptance fixture"}
    write(marker, json.dumps(manifest, indent=2) + "\n")
    return manifest


def load(args):
    manifest = json.loads((args.directory / "manifest.json").read_text())
    require(manifest["source"] == str(args.source), "source does not match prepared acceptance")
    return manifest


def build(args):
    manifest = load(args)
    documentation = (args.source / "docs/federation-adapters.md").read_text()
    blocks = re.findall(r"^```go\s*\n(.*?)^```", documentation, re.M | re.S)
    examples = [block for block in blocks if block.startswith("package staticadapter\n")]
    require(len(examples) == 1 and hashlib.sha256(examples[0].encode()).hexdigest() == manifest["example_sha256"],
            "documented example changed after prepare; use a fresh acceptance directory")
    original = args.source / "artifacts/duckbridge"
    provisioned = json.loads((original / "build.json").read_text())
    snapshot = args.directory / ("source-" + uuid.uuid4().hex)
    shutil.copytree(args.source, snapshot, ignore=shutil.ignore_patterns(".git", "artifacts", "bin", "dist", ".venv", "__pycache__", ".DS_Store"))
    write(snapshot / "cmd/kelvo/adapters_acceptance.go", f'//go:build {TAG}\n\npackage main\n\nimport (\n _ "{MODULE}/staticadapter"\n _ "{MODULE}/fixtureadapter"\n)\n')
    modfile = snapshot / "adapter.mod"
    shutil.copyfile(original / "duckbridge.mod", modfile)
    shutil.copyfile(original / "duckbridge.sum", modfile.with_suffix(".sum"))
    env = environment()
    env.update(CGO_ENABLED="1", CGO_CXXFLAGS=provisioned["CGO_CXXFLAGS"])
    command([args.go, "mod", "edit", "-modfile", str(modfile), "-require", MODULE + "@v0.0.0", "-replace", MODULE + "=" + manifest["external_module"]], snapshot, env, args.directory / "adapter-modfile.log")
    binary = args.directory / "kelvo-adapter-acceptance"
    result = command([args.go, "build", "-p", "2", "-mod=mod", "-modfile", str(modfile), "-tags", "duckdb_arrow,duckbridge," + TAG, "-o", str(binary), "./cmd/kelvo"], snapshot, env, args.directory / "adapter-build.log", timeout=900)
    manifest.update(binary=str(binary), snapshot=str(snapshot), build=result)
    write(args.directory / "manifest.json", json.dumps(manifest, indent=2) + "\n")
    return manifest


def run(args):
    manifest = load(args)
    binary = Path(manifest["binary"])
    require(binary.is_file(), "build the adapter-enabled binary before running acceptance")
    directory = args.directory / ("results-" + uuid.uuid4().hex)
    directory.mkdir()
    write(directory / "tags.csv", "id,tag\n1,one\n2,two\n3,three\n4,four\n")
    config = {"sources": [
        {"id": "demo", "type": "example_static", "federation": {"tables": [{"name": "sample", "table": "one_row"}]}},
        {"id": "custom", "type": "acceptance_rows", "federation": {"max_scan_rows": 10, "tables": [{"name": "rows", "table": "rows"}]}},
        {"id": "tags", "type": "csv", "path": str(directory / "tags.csv")},
    ]}
    catalog = directory / "catalog.json"
    write(catalog, json.dumps(config))
    cases = []

    def query(name, sql, selected, expected=None, max_rows=10, config_file=catalog, success=True):
        output = directory / (name + ".arrow")
        argv = [str(binary), "query", "--config", str(config_file), "--sources", ",".join(selected), "--sql", sql,
                "--out", str(output), "--max-rows", str(max_rows), "--max-bytes", "1048576", "--timeout", "15s",
                "--memory-mb", "256", "--threads", "2", "--temp-mb", "64"]
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
            raise AssertionError(f"CLI query exceeded its acceptance deadline: {name}") from None
        write(directory / (name + ".log"), stdout.decode() + stderr.decode())
        case = {"name": name, "sql": sql, "coordinator_pid": process.pid, "exit_code": process.returncode}
        if success:
            require(process.returncode == 0 and output.is_file(), f"CLI query failed: {name}; inspect {directory}")
            decoded = json.loads(subprocess.check_output([str(args.directory / "decode-arrow"), str(output)], text=True))
            if expected is not None:
                require(decoded["rows"] == expected, f"exact result differs for {name}: {decoded}")
            case.update(result=decoded, stats=json.loads(stderr))
        else:
            require(process.returncode != 0 and not output.exists(), f"failed query published a result: {name}")
            case["error"] = stderr.decode().strip()
        cases.append(case)
        return case

    query("documented_example", "SELECT id FROM demo.sample", ["demo"], [[1]])
    query("documented_count", "SELECT count(*) FROM demo.sample", ["demo"], [[1]])
    query("documented_filter_rejection", "SELECT id FROM demo.sample WHERE id=1", ["demo"], success=False)
    query("csv_join", "SELECT r.id,t.tag,r.label FROM custom.rows r JOIN tags t USING(id) WHERE r.id>=2 ORDER BY r.id", ["custom", "tags"], [[2, "two", "drop"], [3, "three", "keep"], [4, "four", None]])
    pushed = query("integer_pushdown", "SELECT id FROM custom.rows WHERE id>=3 ORDER BY id", ["custom"], [[3], [4]])
    require(pushed["stats"]["federation"][0]["rows_fetched"] == 2, "integer filter did not reduce source rows")
    local = query("local_string_filter", "SELECT id FROM custom.rows WHERE label='keep' ORDER BY id", ["custom"], [[1], [3]])
    require(local["stats"]["federation"][0]["rows_fetched"] == 4, "string predicate did not remain local")
    query("count_rows", "SELECT count(*) FROM custom.rows", ["custom"], [[4]])
    query("empty_count", "SELECT count(*) FROM custom.rows WHERE id<0", ["custom"], [[0]])
    worker = query("reexec_worker", "SELECT DISTINCT worker_pid FROM custom.rows", ["custom"])
    pids = worker["result"]["rows"]
    require(len(pids) == 1 and pids[0][0] > 0 and pids[0][0] != worker["coordinator_pid"], "adapter did not execute in a separate worker process")
    limited = json.loads(json.dumps(config))
    limited["sources"][1]["federation"]["max_scan_rows"] = 2
    limited_catalog = directory / "limited-catalog.json"
    write(limited_catalog, json.dumps(limited))
    query("scan_limit", "SELECT count(*) FROM custom.rows", ["custom"], config_file=limited_catalog, success=False)
    query("result_limit", "SELECT id FROM custom.rows ORDER BY id", ["custom"], max_rows=2, success=False)

    # Check structured worker errors independently of CLI's plain public text.
    for name, worker_config, sql, wanted in [
        ("worker_scan_limit", limited, "SELECT count(*) FROM custom.rows", "RESOURCE_EXHAUSTED"),
        ("worker_documented_filter", config, "SELECT id FROM demo.sample WHERE id=1", "UNSUPPORTED"),
    ]:
        selected = "demo" if "demo.sample" in sql else "custom"
        payload = {"config": {"sources": [s for s in worker_config["sources"] if s["id"] == selected]},
                   "limits": {"max_rows": 10, "max_bytes": 1048576, "timeout": 15000000000, "memory_mb": 256, "threads": 2, "max_temp_mb": 64},
                   "request": {"mode": "federated", "sources": [selected], "sql": sql}}
        result = subprocess.run([str(binary), "worker"], input=json.dumps(payload).encode(), capture_output=True, timeout=30, cwd=directory)
        outcome = json.loads(result.stderr)
        require(result.returncode == 0 and outcome.get("error", {}).get("code") == wanted, f"worker lost typed error for {name}: {outcome}")
        cases.append({"name": name, "outcome": outcome, "output_bytes": len(result.stdout)})

    source_files = ["federation/federation.go", "federation/registry.go", "internal/federation/custom.go", "internal/duckbridge/shim.cpp"]
    report = {"status": "passed", "checked_at": datetime.now(timezone.utc).isoformat(),
              "example_sha256": manifest["example_sha256"], "binary": str(binary), "binary_sha256": digest(binary),
              "source_sha256": {name: digest(Path(manifest["snapshot"]) / name) for name in source_files},
              "public_import_boundary_verified": True, "repository_imports": manifest["repository_imports"],
              "cases": cases, "scope": manifest["scope"], "not_a_live_database_test": True}
    write(directory / "report.json", json.dumps(report, indent=2) + "\n")
    manifest["report"] = str(directory / "report.json")
    write(args.directory / "manifest.json", json.dumps(manifest, indent=2) + "\n")
    return {"status": "passed", "cases": len(cases), "report": manifest["report"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "build", "run"))
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--go", default="go")
    args = parser.parse_args()
    require(sys.platform == "linux" and os.uname().machine == "x86_64", "run this build acceptance only on the designated Linux amd64 VM")
    args.source, args.directory = args.source.resolve(), args.directory.resolve()
    require(not args.directory.is_relative_to(args.source), "acceptance scratch must be outside the tested source checkout")
    require(args.directory != args.source.parent, "acceptance needs its own dedicated scratch directory")
    result = {"prepare": prepare, "build": build, "run": run}[args.phase](args)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
