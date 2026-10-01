#!/usr/bin/env python3
"""Provision the optional pinned C++ Arrow shim inputs on the build VM only.

Does not modify the baseline go.mod or install an unsigned DuckDB extension.
The returned JSON names a separate module file and required C++ include flags.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tarfile
import urllib.request

VERSION = "v1.5.6"
ARCHIVE_URL = "https://codeload.github.com/duckdb/duckdb/tar.gz/refs/tags/v1.5.6"
ARCHIVE_SHA256 = "1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b"
MODULE = "github.com/duckdb/duckdb-go/v2"
MODULE_VERSION = "v2.10506.0"
MODULE_SUM = "h1:mcZjUQ/kSNeZdtGCqyj5/JEDUdj7/kvILfIOUTzQIu0="


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def apply_driver_patch(patched, patch, *options):
    # A copied module is not a Git repository. Without a discovery ceiling,
    # artifacts nested in a checkout inherit its .git, and git apply silently
    # skips this module-relative patch while still returning success.
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("GIT_")}
    environment["GIT_CEILING_DIRECTORIES"] = str(patched.resolve().parent)
    subprocess.run(["git", "apply", *options, str(patch.resolve())],
                   cwd=patched, env=environment, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=pathlib.Path)
    parser.add_argument("--go", default="go")
    parser.add_argument("--source-directory", type=pathlib.Path,
                        default=pathlib.Path(__file__).resolve().parent.parent)
    args = parser.parse_args()
    if os.uname().sysname != "Linux" or os.uname().machine != "x86_64":
        parser.error("the initial pinned bridge build is Linux amd64 only; run it on the designated VM")
    directory = args.directory.resolve()
    source = args.source_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    archive = directory / "duckdb-v1.5.6.tar.gz"
    if not archive.exists():
        temporary = archive.with_suffix(".download")
        try:
            size = 0
            with urllib.request.urlopen(ARCHIVE_URL, timeout=60) as response, temporary.open("wb") as output:
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    size += len(block)
                    if size > 128 * 1024 * 1024:
                        raise ValueError("DuckDB source archive exceeds its bound")
                    output.write(block)
            if digest(temporary) != ARCHIVE_SHA256:
                raise ValueError("DuckDB source archive does not match its pinned checksum")
            temporary.replace(archive)
        finally:
            temporary.unlink(missing_ok=True)
    if digest(archive) != ARCHIVE_SHA256:
        raise ValueError("DuckDB source archive does not match its pinned checksum")
    headers = directory / "headers"
    headers.mkdir(exist_ok=True)
    prefix = "duckdb-1.5.6/src/include/"
    count, total = 0, 0
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle:
            if not member.name.startswith(prefix) or not member.isfile():
                continue
            relative = pathlib.PurePosixPath(member.name[len(prefix):])
            if ".." in relative.parts or relative.is_absolute() or member.size > 4 * 1024 * 1024:
                raise ValueError("invalid source header archive member")
            total += member.size
            if total > 64 * 1024 * 1024:
                raise ValueError("source headers exceed their extraction bound")
            destination = headers.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as input_file, destination.open("wb") as output:
                shutil.copyfileobj(input_file, output)
            count += 1
    if count < 100 or not (headers / "duckdb/function/table/arrow.hpp").is_file():
        raise ValueError("pinned source headers are incomplete")
    module = json.loads(subprocess.check_output(
        [args.go, "mod", "download", "-json", f"{MODULE}@{MODULE_VERSION}"], cwd=source, text=True))
    if module.get("Sum") != MODULE_SUM or module.get("Version") != MODULE_VERSION:
        raise ValueError("DuckDB Go module does not match its pinned checksum")
    patched = directory / "duckdb-go"
    patch = source / "scripts/duckbridge-driver.patch"
    patch_sha = digest(patch)
    marker = directory / "driver-patch.sha256"
    if patched.exists():
        if not marker.exists() or marker.read_text().strip() != patch_sha:
            raise ValueError("existing optional driver has a different patch; use a fresh artifact directory")
    else:
        shutil.copytree(module["Dir"], patched)
        patched.chmod(0o755)
        apply_driver_patch(patched, patch, "--check")
        apply_driver_patch(patched, patch)
    # Verify the files even when reusing an artifact with a matching marker:
    # earlier helpers could write the marker after Git had skipped the patch.
    try:
        apply_driver_patch(patched, patch, "--reverse", "--check")
    except subprocess.CalledProcessError as error:
        raise ValueError("optional driver patch is missing or changed; use a fresh artifact directory") from error
    marker.write_text(patch_sha + "\n")
    modfile = directory / "duckbridge.mod"
    shutil.copyfile(source / "go.mod", modfile)
    shutil.copyfile(source / "go.sum", modfile.with_suffix(".sum"))
    subprocess.run([args.go, "mod", "edit", "-modfile", str(modfile),
                    "-replace", f"{MODULE}={patched}"], cwd=source, check=True)
    result = {
        "duckdb": VERSION, "driver": MODULE_VERSION, "source_sha256": ARCHIVE_SHA256,
        "module_sum": MODULE_SUM, "driver_patch_sha256": patch_sha,
        "headers": str(headers), "header_count": count,
        "modfile": str(modfile), "tags": "duckdb_arrow,duckbridge",
        "CGO_CXXFLAGS": f"-I{headers}",
    }
    (directory / "build.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
