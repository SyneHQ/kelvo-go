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
    args = p.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for alias in ("postgres", "mysql"):
        url = f"https://extensions.duckdb.org/{args.version}/{args.platform}/{alias}_scanner.duckdb_extension.gz"
        request = urllib.request.Request(url, headers={"User-Agent": f"duckdb/{args.version}"})
        with urllib.request.urlopen(request, timeout=60) as response:
            data = gzip.decompress(response.read(64 * 1024 * 1024))
        destination = args.directory / f"{alias}_scanner.duckdb_extension"
        with tempfile.NamedTemporaryFile(dir=args.directory, delete=False) as output:
            temp = pathlib.Path(output.name)
            output.write(data)
        try:
            temp.chmod(0o644)
            os.replace(temp, destination)
        finally:
            temp.unlink(missing_ok=True)
        manifest[alias] = {"url": url, "sha256": hashlib.sha256(data).hexdigest()}
    (args.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Extensions downloaded; DuckDB validates signatures when loading them.")

if __name__ == "__main__":
    main()
