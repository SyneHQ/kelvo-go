#!/usr/bin/env python3
"""Provision signed DuckDB extensions before running Kelvo; never during a query."""
import argparse
import gzip
import hashlib
import json
import os
import pathlib
import tempfile
import urllib.request

def main():
    p = argparse.ArgumentParser()
    p.add_argument("directory", type=pathlib.Path)
    p.add_argument("--version", default="v1.5.6", choices=["v1.5.6"])
    p.add_argument("--platform", default="linux_amd64", choices=["linux_amd64"])
    p.add_argument("--extensions", default="postgres,mysql,sqlite",
                   help="Comma-separated approved extensions: postgres,mysql,sqlite,httpfs; object range reads are opt-in")
    args = p.parse_args()
    names = {"postgres": "postgres_scanner", "mysql": "mysql_scanner",
             "sqlite": "sqlite_scanner", "httpfs": "httpfs"}
    selected = list(dict.fromkeys(part.strip() for part in args.extensions.split(",")))
    if not selected or any(name not in names for name in selected):
        p.error("--extensions must name only approved extensions: postgres,mysql,sqlite,httpfs")
    args.directory.mkdir(parents=True, exist_ok=True)
    manifest = {}
    manifest_path = args.directory / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if not isinstance(manifest, dict):
            raise ValueError("existing extension manifest must be an object")
    for alias in selected:
        name = names[alias]
        url = f"https://extensions.duckdb.org/{args.version}/{args.platform}/{name}.duckdb_extension.gz"
        request = urllib.request.Request(url, headers={"User-Agent": f"duckdb/{args.version}"})
        with urllib.request.urlopen(request, timeout=60) as response:
            data = gzip.decompress(response.read(64 * 1024 * 1024))
        destination = args.directory / f"{name}.duckdb_extension"
        with tempfile.NamedTemporaryFile(dir=args.directory, delete=False) as output:
            temp = pathlib.Path(output.name)
            output.write(data)
        try:
            temp.chmod(0o644)
            os.replace(temp, destination)
        finally:
            temp.unlink(missing_ok=True)
        manifest[alias] = {"url": url, "sha256": hashlib.sha256(data).hexdigest()}
    with tempfile.NamedTemporaryFile(dir=args.directory, delete=False) as output:
        temp = pathlib.Path(output.name)
        output.write((json.dumps(manifest, indent=2) + "\n").encode())
    try:
        temp.chmod(0o644)
        os.replace(temp, manifest_path)
    finally:
        temp.unlink(missing_ok=True)
    print("Extensions downloaded; DuckDB validates signatures when loading them.")

if __name__ == "__main__":
    main()
