#!/usr/bin/env python3
"""Build exact same-source client variants on a provisioned Linux test VM.

Downloads nothing. Uses cached modules and the operator's already validated
DuckDB bridge modfile/headers. Source go.mod/go.sum and bridge remain unchanged.
Private build logs and external modfiles remain under the fresh output root.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

from config_refusal_acceptance import isolation_evidence
from operational_acceptance import require, sha256

SUMS = {
    "v1.53.1": ("h1:Otsq3uLc/kLdjmkNHkXH0jBqwUquwdKFoe3fq6/3/Xo=", "h1:26HypzazeOkyO3/mqd1zZd53STJN0EjCYF9Uy2ZOBno="),
    "v1.54.0": ("h1:vsXoOxjHp/GmPUN+EcI7uOf/uB+iAP+kEsAFNQN0yzA=", None),
}


def tree_digest(root):
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if ".git" in relative.parts:
            continue
        require(not path.is_symlink(), "SOURCE_TREE_SYMLINK")
        if path.is_file():
            digest.update(str(relative).encode()+b"\0"+sha256(path).encode()+b"\n")
    return digest.hexdigest()


def linked_modules(build_info):
    modules = []
    for line in build_info.splitlines()[1:]:
        fields = line.strip().split("\t")
        if fields[0] == "dep":
            require(len(fields) in (3, 4), "INVALID_EMBEDDED_MODULE")
            modules.append(dict(path=fields[1], version=fields[2], sum=fields[3] if len(fields) == 4 else None,
                                replacement=None))
        elif fields[0] == "=>":
            require(bool(modules) and len(fields) >= 2, "INVALID_EMBEDDED_REPLACEMENT")
            modules[-1]["replacement"] = {"path": fields[1], "version": fields[2] if len(fields) > 2 else None}
    return modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "output", "validated-modfile", "native-headers", "go", "sandbox", "source-archive", "revision"):
        parser.add_argument("--"+name, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    isolation = isolation_evidence()
    source, output, headers = map(lambda p: Path(p).resolve(), (args.source, args.output, args.native_headers))
    require(re.fullmatch(r"[a-f0-9]{40}", args.revision) is not None, "EXACT_REVISION_REQUIRED")
    require(not output.exists() and not output.is_relative_to(source), "FRESH_EXTERNAL_BUILD_DIRECTORY_REQUIRED")
    output.mkdir(mode=0o700)
    env = dict(os.environ, GOMAXPROCS="1", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off", GOWORK="off",
               CGO_CXXFLAGS="-I"+str(headers))
    validated = Path(args.validated_modfile).resolve()
    default_before = {name: sha256(source/name) for name in ("go.mod", "go.sum")}
    source_before = tree_digest(source)
    raw_config = subprocess.check_output([args.go, "mod", "edit", "-json", "-modfile", str(validated)], cwd=source, env=env)
    config = json.loads(raw_config)
    replacements = config.get("Replace", [])
    require(len(replacements) == 1 and replacements[0]["Old"]["Path"] == "github.com/duckdb/duckdb-go/v2",
            "UNDECLARED_NATIVE_REPLACEMENTS")
    bridge = Path(replacements[0]["New"]["Path"]).resolve()
    bridge_before, headers_before = tree_digest(bridge), tree_digest(headers)
    archive_sha, launcher_sha = sha256(args.source_archive), sha256(args.sandbox)
    manifest = dict(schema_version=1, passed=False, revision=args.revision, checked_at=datetime.now(timezone.utc).isoformat(),
                    enforced_isolation=isolation, source_archive_sha256=archive_sha, source_tree_sha256=source_before,
                    default_modfile_sha256=default_before, bridge_tree_sha256=bridge_before, headers_tree_sha256=headers_before,
                    builds={}, build_tags=["duckdb_arrow", "duckbridge"], trimpath=True, source_dependencies_unchanged=False)
    report_path = output/"build-evidence.json"
    manifests = {}
    try:
        for label, version in (("new", "v1.54.0"), ("old", "v1.53.1")):
            started = time.monotonic()
            modfile = output/(label+".mod")
            shutil.copyfile(validated, modfile)
            shutil.copyfile(validated.with_suffix(".sum"), modfile.with_suffix(".sum"))
            if label == "old":
                subprocess.run([args.go, "mod", "edit", "-modfile", str(modfile), "-require=github.com/nats-io/nats.go@"+version],
                               cwd=source, env=env, check=True)
                with modfile.with_suffix(".sum").open("a") as sums:
                    sums.write("github.com/nats-io/nats.go "+version+" "+SUMS[version][0]+"\n")
                    sums.write("github.com/nats-io/nats.go "+version+"/go.mod "+SUMS[version][1]+"\n")
            effective = json.loads(subprocess.check_output([args.go, "mod", "edit", "-json", "-modfile", str(modfile)],
                                                            cwd=source, env=env))
            original_require = {entry["Path"]: entry for entry in config["Require"]}
            effective_require = {entry["Path"]: entry for entry in effective["Require"]}
            expected_require = dict(original_require)
            expected_require["github.com/nats-io/nats.go"] = dict(original_require["github.com/nats-io/nats.go"], Version=version)
            require(effective_require == expected_require and effective.get("Replace") == config.get("Replace"),
                    "UNDECLARED_REQUIRE_OR_REPLACE_CHANGE")
            sets_path = output/(label+"-effective-require-replace.json")
            sets_path.write_text(json.dumps({"require": effective["Require"], "replace": effective["Replace"]}, indent=2)+"\n")
            binary = output/(label+"-kelvo")
            with (output/(label+"-build-private.log")).open("wb") as log:
                subprocess.run([args.go, "build", "-p", "1", "-mod=readonly", "-modfile", str(modfile), "-trimpath",
                                "-tags", "duckdb_arrow,duckbridge", "-o", str(binary), "./cmd/kelvo"],
                               cwd=source, env=env, stdout=log, stderr=log, check=True, timeout=1200)
            build_info = subprocess.check_output([args.go, "version", "-m", str(binary)], env=env).decode()
            require("github.com/nats-io/nats.go\t"+version+"\t" in build_info, "EMBEDDED_CLIENT_VERSION_MISMATCH")
            (output/(label+"-binary-modules.txt")).write_text(build_info)
            modules = linked_modules(build_info)
            selected = next(m for m in modules if m["path"] == "github.com/nats-io/nats.go")
            require(selected["version"] == version and selected["sum"] == SUMS[version][0], "CLIENT_SUM_MISMATCH")
            # Use linked modules from the actual full binary. Listing the entire
            # module graph would require unrelated, unused test dependencies.
            modules_path = output/(label+"-modules.json")
            modules_path.write_text(json.dumps(modules, indent=2)+"\n")
            manifests[label] = {m["path"]: (m["version"], m["sum"], m["replacement"]) for m in modules}
            manifest["builds"][label] = dict(nats_client=version, binary_sha256=sha256(binary), source_archive_sha256=archive_sha,
                      launcher_sha256=launcher_sha, module_manifest_sha256=sha256(modules_path),
                      effective_modfile_sha256=sha256(modfile), effective_sumfile_sha256=sha256(modfile.with_suffix(".sum")),
                      effective_require_replace_sha256=sha256(sets_path),
                      elapsed_seconds=round(time.monotonic()-started, 3))
            report_path.write_text(json.dumps(manifest, indent=2)+"\n")
            print(label+"_full_engine_build_passed", flush=True)
        changed = sorted(p for p in set(manifests["old"]) | set(manifests["new"]) if manifests["old"].get(p) != manifests["new"].get(p))
        require(changed == ["github.com/nats-io/nats.go"], "UNDECLARED_TRANSITIVE_VERSION_CHANGE")
        require(default_before == {name: sha256(source/name) for name in default_before} and source_before == tree_digest(source)
                and bridge_before == tree_digest(bridge) and headers_before == tree_digest(headers), "SOURCE_INPUT_MUTATED")
        manifest.update(passed=True, source_dependencies_unchanged=True, changed_modules=changed)
        matrix = dict(mode="client", old_revision=args.revision, new_revision=args.revision,
                      default_dependencies_unchanged=True, old=manifest["builds"]["old"], new=manifest["builds"]["new"],
                      bridge_tree_sha256=bridge_before, headers_tree_sha256=headers_before)
        (output/"client-matrix.json").write_text(json.dumps(matrix, indent=2)+"\n")
    except BaseException as error:
        manifest["failure_type"] = type(error).__name__
        raise
    finally:
        report_path.write_text(json.dumps(manifest, indent=2)+"\n")


if __name__ == "__main__":
    main()
