#!/usr/bin/env python3
"""Prepare disposable collector trust. Does not install it or launch services."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(directory, system_bundle):
    if directory.exists() or directory.is_symlink():
        raise RuntimeError("FRESH_CA_DIRECTORY_REQUIRED")
    if system_bundle.is_symlink() or not system_bundle.is_file() or not 0 < system_bundle.stat().st_size < 4 << 20:
        raise RuntimeError("REGULAR_SYSTEM_CA_BUNDLE_REQUIRED")
    original = system_bundle.read_bytes()
    directory.mkdir(mode=0o700, parents=False)
    previous_umask = os.umask(0o077)
    try:
        def openssl(*args):
            subprocess.run(["openssl", *args], cwd=directory, check=True, timeout=20,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key",
                "-out", "ca.pem", "-days", "2", "-subj", "/CN=Kelvo OTLP fixture CA",
                "-addext", "basicConstraints=critical,CA:true", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", "collector.key",
                "-out", "collector.csr", "-subj", "/CN=Kelvo OTLP fixture")
        (directory / "collector.ext").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
        openssl("x509", "-req", "-in", "collector.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                "-CAcreateserial", "-out", "collector.pem", "-days", "2", "-extfile", "collector.ext")
        (directory / "ca-bundle.pem").write_bytes(original.rstrip(b"\n") + b"\n" + (directory / "ca.pem").read_bytes())
        (directory / "manifest.json").write_text(json.dumps({
            "system_bundle_before_sha256": hashlib.sha256(original).hexdigest(),
            "prepared_bundle_sha256": digest(directory / "ca-bundle.pem"),
            "collector_certificate_sha256": digest(directory / "collector.pem"),
        }, indent=2) + "\n")
    finally:
        os.umask(previous_umask)
    if system_bundle.read_bytes() != original:
        raise RuntimeError("HOST_TRUST_CHANGED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--system-bundle", type=Path, default=Path("/etc/ssl/certs/ca-certificates.crt"))
    args = parser.parse_args()
    prepare(args.directory.resolve(), args.system_bundle)
