#!/usr/bin/env python3
"""Research ONLY: fail-closed Rustdoc link-hint fixture generator.

Not a replacement for dependency documentation or a production API-doc build.
Call only with a NEW, isolated CARGO_TARGET_DIR/doc. Nothing is published.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

LIB_KINDS = frozenset({"lib", "rlib", "dylib", "proc-macro"})
CRATE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")

# Exact *currently audited* multi-version inventory from pinned Cargo.lock.
# An added, removed, or upgraded collision fails closed until re-reviewed.
REVIEWED_DUPLICATES = {
    "block_buffer": ("0.10.4", "0.12.1"),
    "cpufeatures": ("0.2.17", "0.3.0"),
    "crypto_common": ("0.1.6", "0.2.2"),
    "digest": ("0.10.7", "0.11.3"),
    "getrandom": ("0.2.17", "0.4.3"),
    "hashbrown": ("0.16.1", "0.17.1"),
    "sha2": ("0.10.9", "0.11.0"),
    "syn": ("2.0.119", "3.0.5"),
    "windows_sys": ("0.52.0", "0.61.2"),
}
PLACEHOLDER = b"<!doctype html><meta charset=\"utf-8\"><title>research-only Rustdoc link anchor; NOT documentation</title>\n"


class HintError(ValueError):
    """Reject potentially ambiguous or destructive fixture generation."""


def plan(metadata: object) -> tuple[list[str], dict[str, tuple[str, ...]]]:
    if not isinstance(metadata, dict):
        raise HintError("metadata root is not an object")
    packages = metadata.get("packages")
    resolved = metadata.get("resolve")
    if not isinstance(packages, list) or not packages:
        raise HintError("missing metadata packages")
    if not isinstance(resolved, dict) or not isinstance(resolved.get("nodes"), list):
        raise HintError("missing resolved dependency graph")

    nodes = {x.get("id") for x in resolved["nodes"] if isinstance(x, dict)}
    ids = [p.get("id") for p in packages if isinstance(p, dict)]
    if len(ids) != len(packages) or len(ids) != len(set(ids)) or nodes != set(ids):
        raise HintError("incomplete, duplicate, or mismatched resolved package ids")

    versions: dict[str, set[str]] = defaultdict(set)
    for package in packages:
        version = package.get("version")
        targets = package.get("targets")
        if not isinstance(version, str) or not isinstance(targets, list):
            raise HintError("package lacks version or targets")
        for target in targets:
            kinds = target.get("kind", []) if isinstance(target, dict) else []
            if not isinstance(kinds, list):
                raise HintError("invalid target kinds")
            if not (LIB_KINDS & set(kinds)):
                continue
            name = target.get("name")
            if not isinstance(name, str) or not CRATE_NAME.fullmatch(name):
                raise HintError(f"unsafe or invalid Cargo library target name: {name!r}")
            versions[name].add(version)

    if not versions:
        raise HintError("graph has no library targets")
    duplicates = {n: tuple(sorted(v)) for n, v in versions.items() if len(v) > 1}
    for name, found in duplicates.items():
        if REVIEWED_DUPLICATES.get(name) != found:
            raise HintError(f"unreviewed duplicate crate name: {name}, versions={found}")
    # Detect version collision drift, including disappeared historical copies.
    for name, known in REVIEWED_DUPLICATES.items():
        if name in versions and len(versions[name]) > 1 and duplicates[name] != known:
            raise HintError(f"reviewed collision changed: {name}")
    return sorted(versions), duplicates


def generate(metadata: object, destination: Path) -> dict[str, object]:
    """Write all-or-nothing link hints into a previously absent document root."""
    names, collisions = plan(metadata)  # Full validation before any filesystem change.
    if not destination.is_absolute() or destination.name != "doc":
        raise HintError("destination must be an absolute, new CARGO_TARGET_DIR/doc")
    if destination.exists() or destination.is_symlink():
        raise HintError("refusing to modify existing documents or a symlink")
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise HintError("CARGO_TARGET_DIR must exist and not be a symlink")
    stage = Path(tempfile.mkdtemp(prefix=".rustdoc-hints-stage-", dir=parent))
    try:
        for name in names:
            folder = stage / name
            folder.mkdir()
            (folder / "index.html").write_bytes(PLACEHOLDER)
        if destination.exists() or destination.is_symlink():
            raise HintError("document destination appeared concurrently")
        stage.rename(destination)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return {
        "schema": "rustdoc-research-hints-v1",
        "crate_names": len(names),
        "reviewed_collisions": {n: list(v) for n, v in sorted(collisions.items())},
        "documentation_is_real": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        raw = subprocess.check_output(
            ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
            text=True,
        )
        report = generate(json.loads(raw), args.doc_root)
    except (HintError, json.JSONDecodeError, subprocess.SubprocessError, OSError) as exc:
        parser.exit(2, f"RUSTDOC_HINTS_REJECTED: {exc}\n")
    print("RUSTDOC_HINTS_CREATED " + json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
