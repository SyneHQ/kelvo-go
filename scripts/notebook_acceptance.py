#!/usr/bin/env python3
"""Execute Kelvo's complete public-data notebook curriculum in fresh kernels.

Install notebook requirements, nbformat, nbclient and ipykernel on the designated
validation VM first. This runner never installs packages, fabricates outputs or
changes the checked-in notebooks. Reports include failures; partial runs are
explicitly distinguished from validation of all 15 lessons.
Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
import time

NOTEBOOK_COUNT = 15
PACKAGES = ("nbformat", "nbclient", "ipykernel", "pyarrow", "pandas", "matplotlib", "PyYAML")
FORBIDDEN_TAGS = frozenset({"skip-execution", "raises-exception"})


class ValidationError(Exception):
    """Messages are controlled diagnostics, never runtime exception strings."""


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def code_text(cell):
    value = cell.get("source", "")
    return "".join(value) if isinstance(value, list) else value


def expected_code_cells(notebook):
    return [cell for cell in notebook["cells"]
            if cell["cell_type"] == "code" and code_text(cell).strip()]


def validate_notebook(path, *, allow_unpinned=False, helper_sha256=None):
    """Reject committed outputs, execution-skipping metadata and unfrozen helpers."""
    import nbformat

    raw = json.loads(Path(path).read_text())
    # nbformat can normalize duplicate ids. Check the source before normalization.
    identities = [cell.get("id") for cell in raw.get("cells", [])]
    if not identities or any(not identity for identity in identities) or len(set(identities)) != len(identities):
        raise ValidationError("Notebook cells require unique explicit ids")
    notebook = nbformat.read(path, as_version=4)
    nbformat.validate(notebook)
    if notebook.nbformat != 4:
        raise ValidationError("Expected notebook format 4")
    bootstrap, cleanup = [], []
    for cell in notebook.cells:
        tags = set(cell.get("metadata", {}).get("tags", []))
        if tags & FORBIDDEN_TAGS:
            raise ValidationError("A source cell requests skipped execution or permitted errors")
        if "bootstrap" in tags:
            bootstrap.append(cell)
        if "cleanup" in tags:
            cleanup.append(cell)
        if cell.cell_type == "code":
            if cell.get("outputs") or cell.get("execution_count") is not None:
                raise ValidationError("Checked-in notebook outputs must be cleared")
            ast.parse(cell.source)
    if len(bootstrap) != 1 or len(cleanup) != 1 or not expected_code_cells(notebook):
        raise ValidationError("Expected one bootstrap, one cleanup and executable cells")
    assignments = {}
    for node in ast.walk(ast.parse(bootstrap[0].source)):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = node.value.value
    reference = assignments.get("KELVO_NOTEBOOK_REF", "")
    digest = assignments.get("KELVO_HELPER_SHA256", "")
    frozen = (isinstance(reference, str) and re.fullmatch(r"[0-9a-f]{40}", reference) is not None
              and isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None)
    if frozen and helper_sha256 is not None and digest != helper_sha256:
        if not allow_unpinned:
            raise ValidationError("Frozen helper digest differs from the validation helper")
        frozen = False
    if not frozen and not allow_unpinned:
        raise ValidationError("Notebook helper reference and digest are not frozen")
    return notebook, {"helper_reference": reference, "helper_expected_sha256": digest, "pins_verified": frozen}


def verify_execution(notebook):
    """An error-free return alone is insufficient: every nonempty cell must run."""
    cells = expected_code_cells(notebook)
    counts = [cell.get("execution_count") for cell in cells]
    if counts != list(range(1, len(cells) + 1)):
        raise ValidationError("Not every source code cell executed exactly once in the fresh kernel")
    if any(output.get("output_type") == "error" for cell in cells for output in cell.get("outputs", [])):
        raise ValidationError("Executed notebook contains an error output")


def kernel_environment(binary, helper, data_cache, *, public_bootstrap=False):
    environment = os.environ.copy()
    # Acceptance must execute notebook answer assertions even if the launcher
    # environment normally requests optimized Python.
    environment.pop("PYTHONOPTIMIZE", None)
    # Always exercise the self-contained route; inherited secrets must not turn
    # a validation run into requests against an operator's real database.
    for name in list(environment):
        if name.startswith("KELVO_REMOTE_"):
            environment.pop(name, None)
    overrides = {
        "KELVO_BINARY": str(binary),
        "KELVO_NOTEBOOK_HELPER": str(helper),
        "KELVO_NOTEBOOK_SKIP_INSTALL": "1",
    }
    if public_bootstrap:
        for name in overrides:
            environment.pop(name, None)
    else:
        environment.update(overrides)
    environment.update({
        "KELVO_NOTEBOOK_DATA": str(data_cache),
        "MPLBACKEND": "module://matplotlib_inline.backend_inline",
        "PYTHONUNBUFFERED": "1",
    })
    return environment


def execute_notebook(notebook, *, work_directory, environment, cell_timeout):
    """Pin the kernel interpreter to this runner's installed virtual environment."""
    from jupyter_client import KernelManager
    from jupyter_client.kernelspec import KernelSpecManager
    from nbclient import NotebookClient

    with tempfile.TemporaryDirectory(prefix="kernel-", dir=work_directory) as kernel_root:
        root = Path(kernel_root)
        spec = root / "kelvo-acceptance"
        spec.mkdir()
        (spec / "kernel.json").write_text(json.dumps({
            "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
            "display_name": "Kelvo acceptance (runner Python)", "language": "python",
        }))
        manager = KernelManager(kernel_name="kelvo-acceptance",
                                kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(root)]))
        client = NotebookClient(notebook, km=manager, timeout=cell_timeout,
                                startup_timeout=60, allow_errors=False, force_raise_errors=True,
                                record_timing=True)
        # Supplying a KernelManager otherwise makes nbclient leave the kernel
        # alive. Explicit cleanup applies on both success and cell exceptions.
        client.execute(cwd=str(work_directory), env=environment, cleanup_kc=True)
    return notebook


def load_helper(path):
    spec = importlib.util.spec_from_file_location("kelvo_acceptance_helper", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def dataset_provenance(cache, datasets):
    result = []
    for name, (url, expected, limit) in sorted(datasets.items()):
        path = cache / name
        if Path(name).name != name or path.is_symlink() or not path.is_file():
            raise ValidationError("Required dataset cache entry is missing or not a regular file")
        size = path.stat().st_size
        digest = sha256(path)
        if size > limit or digest != expected:
            raise ValidationError("Required dataset cache entry failed its size or digest check")
        result.append({"name": name, "url": url, "bytes": size,
                       "sha256": digest, "expected_sha256": expected})
    return result


def prepare_output(repo, requested):
    """Keep rendered output outside source notebooks and never overwrite a run."""
    base = repo / "artifacts"
    destination = Path(requested)
    if not destination.is_absolute():
        destination = repo / destination
    destination = destination.resolve()
    if base.is_symlink() or not destination.is_relative_to(base.resolve()) or destination == base.resolve():
        raise ValidationError("Output must be a new directory beneath repository artifacts")
    base.mkdir(exist_ok=True)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    return destination


def save_report(path, report):
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def public_failure(error):
    # Do not persist arbitrary exception text: it can include private process
    # arguments, environment values or credentials. Executed public-data copies
    # contain detailed cell failures for local operator debugging.
    value = {"type": type(error).__name__}
    if isinstance(error, ValidationError):
        value["reason"] = str(error)
    return value


def execute_case(path, notebook, *, directory, environment, cell_timeout, pins, execute=execute_notebook):
    import nbformat

    original_digest = sha256(path)
    case = {"notebook": path.name, "source_sha256": original_digest, **pins,
            "expected_code_cells": len(expected_code_cells(notebook)), "status": "running"}
    started = time.monotonic()
    try:
        execute(notebook, work_directory=directory, environment=environment, cell_timeout=cell_timeout)
        verify_execution(notebook)
        if sha256(path) != original_digest:
            raise ValidationError("A checked-in notebook changed during execution")
        case["status"] = "passed"
    except Exception as error:
        case["status"] = "failed"
        case["error"] = public_failure(error)
    finally:
        case["elapsed_seconds"] = round(time.monotonic() - started, 6)
        case["executed_code_cells"] = sum(cell.get("execution_count") is not None for cell in expected_code_cells(notebook))
        # A failed cell's error output stays in its executed artifact, never the
        # source notebook. This makes failures inspectable and non-successful.
        executed = directory / path.name
        nbformat.write(notebook, executed)
        executed.chmod(0o600)
        case["executed_sha256"] = sha256(executed)
        case["executed_artifact"] = executed.name
    return case


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--binary", required=True, type=Path,
                       help="Executed local override, or expected package reference in public-bootstrap mode")
    value.add_argument("--data-cache", required=True, type=Path,
                       help="Existing cache containing the three digest-pinned public datasets")
    value.add_argument("--output-dir", required=True, type=Path,
                       help="New directory beneath this repository's artifacts/")
    value.add_argument("--notebook", action="append", default=[],
                       help="A notebook basename for a focused partial rerun; default is all 15")
    value.add_argument("--cell-timeout", type=int, default=240)
    value.add_argument("--public-bootstrap", action="store_true",
                       help="Execute each default pip/helper/release download path without local overrides")
    value.add_argument("--allow-unpinned", action="store_true", help="Draft validation only; never publication-ready evidence")
    return value


def summarize_cases(report, *, public_bootstrap, allow_unpinned):
    cases = report["notebooks"]
    all_passed = bool(cases) and all(case["status"] == "passed" for case in cases)
    report["status"] = "passed" if all_passed else "failed"
    report["complete_curriculum_passed"] = all_passed and len(cases) == NOTEBOOK_COUNT
    report["frozen_curriculum_passed"] = (report["complete_curriculum_passed"] and not allow_unpinned
                                          and all(case.get("pins_verified") for case in cases))
    report["release_installer_validated"] = public_bootstrap and report["frozen_curriculum_passed"]


def run(args, *, repo=None, execute=execute_notebook):
    repo = (Path(repo) if repo is not None else Path(__file__).resolve().parents[1]).resolve()
    output = prepare_output(repo, args.output_dir)
    report_path = output / "report.json"
    public_bootstrap = args.public_bootstrap
    report = {"schema_version": 1, "started_at": now(), "status": "running", "notebooks": [],
              "expected_curriculum_count": NOTEBOOK_COUNT, "complete_curriculum_passed": False,
              "all_code_cells": {"expected": 0, "executed": 0},
              "frozen_curriculum_passed": False, "allow_unpinned": args.allow_unpinned,
              "release_installer_validated": False,
              "bootstrap_mode": "public_bootstrap" if public_bootstrap else "local_override",
              "execution_boundary": "Kelvo CLI plus authenticated loopback API; isolated Python kernel per notebook",
              "limitations": [("Exercises released helper/archive URLs in isolated Jupyter kernels, not Google's hosted Colab runtime"
                               if public_bootstrap else "Uses explicitly supplied local binary and helper, not the public release installer"),
                              "Validates reproducible notebook answers, not production throughput or tenant isolation",
                              "Optional external gateway branch is disabled; authenticated local gateway branch executes"],
              "runtime": {"python": platform.python_version(), "system": platform.system(),
                          "machine": platform.machine(), "packages": {}}}
    save_report(report_path, report)
    try:
        if not 1 <= args.cell_timeout <= 1800:
            raise ValidationError("Cell timeout must be between 1 and 1800 seconds")
        for package in PACKAGES:
            report["runtime"]["packages"][package] = importlib.metadata.version(package)
        binary = args.binary.expanduser().resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValidationError("Kelvo binary must be an executable regular file")
        report["binary"] = {"name": binary.name, "sha256": sha256(binary), "bytes": binary.stat().st_size,
                            "role": "expected_release_reference" if public_bootstrap else "executed_binary_override"}
        version = subprocess.run([str(binary), "version"], check=True, capture_output=True, text=True, timeout=15)
        report["binary"]["version"] = version.stdout.strip()[:1024]
        helper = repo / "notebooks" / "kelvo_notebooks.py"
        report["helper"] = {"path": "notebooks/kelvo_notebooks.py", "sha256": sha256(helper),
                            "role": "expected_frozen_reference" if public_bootstrap else "executed_helper_override"}
        data_cache = args.data_cache.expanduser().resolve(strict=True)
        report["datasets"] = dataset_provenance(data_cache, load_helper(helper).DATASETS)
        paths = sorted((repo / "notebooks").glob("*.ipynb"))
        if len(paths) != NOTEBOOK_COUNT:
            raise ValidationError("Complete curriculum must contain exactly 15 notebooks")
        if args.notebook:
            requested = set(args.notebook)
            if len(requested) != len(args.notebook) or not requested <= {path.name for path in paths}:
                raise ValidationError("Focused selection must contain unique known notebook basenames")
            paths = [path for path in paths if path.name in requested]
        report["requested_notebooks"] = [path.name for path in paths]
        environment = kernel_environment(binary, helper, data_cache, public_bootstrap=public_bootstrap)
        save_report(report_path, report)
        for path in paths:
            try:
                notebook, pins = validate_notebook(path, allow_unpinned=args.allow_unpinned,
                                                    helper_sha256=report["helper"]["sha256"])
                case = execute_case(path, notebook, directory=output, environment=environment,
                                    cell_timeout=args.cell_timeout, pins=pins, execute=execute)
            except Exception as error:
                case = {"notebook": path.name, "source_sha256": sha256(path),
                        "status": "failed", "stage": "source_validation", "error": public_failure(error)}
            report["notebooks"].append(case)
            save_report(report_path, report)
            print(f"{path.name}: {case['status']}", flush=True)
        summarize_cases(report, public_bootstrap=public_bootstrap, allow_unpinned=args.allow_unpinned)
    except Exception as error:
        report["status"] = "failed"
        report["error"] = public_failure(error)
    finally:
        report["all_code_cells"] = {
            "expected": sum(case.get("expected_code_cells", 0) for case in report["notebooks"]),
            "executed": sum(case.get("executed_code_cells", 0) for case in report["notebooks"]),
        }
        report["finished_at"] = now()
        save_report(report_path, report)
    print(f"Notebook evidence: {report_path}", flush=True)
    return 0 if report["status"] == "passed" else 1


def main():
    try:
        return run(parser().parse_args())
    except (OSError, ValidationError) as error:
        # Failure before an owned artifact directory exists cannot safely emit
        # there; report a controlled category and keep the nonzero exit status.
        print(f"Notebook acceptance setup failed: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
