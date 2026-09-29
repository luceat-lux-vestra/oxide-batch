#!/usr/bin/env python3
"""Fail-closed Rust CodeQL PR-impact classification.

The runtime copy of this script is fetched from the exact pull-request base SHA.
It therefore decides whether PR-head Rust CodeQL may be skipped without trusting
classifier code introduced by the pull request being classified.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterable

API_VERSION = "2022-11-28"
MAX_CHANGED_FILES = 3000
PER_PAGE = 100
ALLOWED_STATUSES = {
    "added",
    "removed",
    "modified",
    "renamed",
    "copied",
    "changed",
    "unchanged",
}
CONTROL_PATHS = {
    ".github/workflows/codeql.yml",
    ".github/scripts/codeql-rust-impact.py",
    ".github/workflows/dependency-review.yml",
    ".github/scripts/validate_actions_security.py",
    ".github/scripts/test_validate_actions_security.py",
    ".github/codeql-config.yml",
}
CONTROL_PREFIXES = (".github/codeql/",)


class ClassificationError(RuntimeError):
    """Raised when exact PR impact cannot be proved."""


@dataclass(frozen=True)
class ChangedFile:
    filename: str
    status: str
    previous_filename: str | None = None

    def paths(self) -> tuple[str, ...]:
        if self.previous_filename:
            return (self.filename, self.previous_filename)
        return (self.filename,)


def rust_impact_path(path: str) -> bool:
    if not path or "\n" in path or "\r" in path:
        raise ClassificationError(f"invalid changed path {path!r}")
    if path.endswith(".rs"):
        return True
    if path == "Cargo.toml" or path.endswith("/Cargo.toml"):
        return True
    if path == "Cargo.lock":
        return True
    if path.startswith(".cargo/"):
        return True
    if path == "rust-toolchain" or path.startswith("rust-toolchain."):
        return True
    if path in CONTROL_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in CONTROL_PREFIXES)


def classify_changed_files(files: Iterable[ChangedFile]) -> tuple[bool, str]:
    for changed in files:
        if changed.status not in ALLOWED_STATUSES:
            raise ClassificationError(
                f"unsupported changed-file status {changed.status!r} for {changed.filename!r}"
            )
        for path in changed.paths():
            if rust_impact_path(path):
                return True, f"rust-impact:{path}"
    return False, "no-rust-impact"


def request_json(api_url: str, repository: str, token: str, path: str, params=None):
    url = f"{api_url.rstrip('/')}/repos/{repository}/{path.lstrip('/')}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "oxide-batch-codeql-rust-impact",
        },
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def load_changed_files(
    api_url: str,
    repository: str,
    token: str,
    pr_number: int,
    base_sha: str,
    head_sha: str,
) -> list[ChangedFile]:
    pr = request_json(api_url, repository, token, f"pulls/{pr_number}")

    if pr.get("base", {}).get("sha") != base_sha:
        raise ClassificationError("live pull request base SHA drifted from the triggering event")
    if pr.get("head", {}).get("sha") != head_sha:
        raise ClassificationError("live pull request head SHA drifted from the triggering event")
    if pr.get("base", {}).get("repo", {}).get("full_name") != repository:
        raise ClassificationError("live pull request base repository is not the protected repository")

    changed_files = pr.get("changed_files")
    if not isinstance(changed_files, int) or changed_files < 0:
        raise ClassificationError("live pull request changed_files is not a non-negative integer")
    if changed_files > MAX_CHANGED_FILES:
        raise ClassificationError(
            f"live pull request has {changed_files} changed files; API proof is capped at {MAX_CHANGED_FILES}"
        )

    records: list[ChangedFile] = []
    page = 1
    while len(records) < changed_files:
        payload = request_json(
            api_url,
            repository,
            token,
            f"pulls/{pr_number}/files",
            {"per_page": PER_PAGE, "page": page},
        )
        if not isinstance(payload, list):
            raise ClassificationError(f"changed-file page {page} is not a JSON array")
        if not payload:
            break

        for item in payload:
            if not isinstance(item, dict):
                raise ClassificationError(f"changed-file page {page} contains a non-object entry")
            filename = item.get("filename")
            status = item.get("status")
            previous = item.get("previous_filename")
            if not isinstance(filename, str) or not filename:
                raise ClassificationError("changed-file entry has no valid filename")
            if not isinstance(status, str) or not status:
                raise ClassificationError(f"{filename}: changed-file entry has no valid status")
            if previous is not None and not isinstance(previous, str):
                raise ClassificationError(f"{filename}: previous_filename is not a string")
            records.append(ChangedFile(filename, status, previous))

        if len(payload) < PER_PAGE:
            break
        page += 1
        if page > (MAX_CHANGED_FILES // PER_PAGE) + 1:
            raise ClassificationError("changed-file pagination exceeded the bounded API envelope")

    if len(records) != changed_files:
        raise ClassificationError(
            f"changed-file reconciliation failed: metadata={changed_files} fetched={len(records)}"
        )
    return records


def write_output(path: str, run_rust: bool, reason: str, changed_files: int) -> None:
    safe_reason = reason.replace("\n", " ").replace("\r", " ")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"run_rust={'true' if run_rust else 'false'}\n")
        handle.write(f"reason={safe_reason}\n")
        handle.write(f"changed_files={changed_files}\n")


def runtime() -> int:
    token = os.environ["GITHUB_TOKEN"]
    api_url = os.environ["GITHUB_API_URL"]
    repository = os.environ["GITHUB_REPOSITORY"]
    pr_number = int(os.environ["PR_NUMBER"])
    base_sha = os.environ["BASE_SHA"]
    head_sha = os.environ["HEAD_SHA"]
    output = os.environ["GITHUB_OUTPUT"]

    if len(base_sha) < 40 or len(head_sha) < 40:
        raise ClassificationError("base/head SHA inputs are not full Git object identifiers")

    files = load_changed_files(
        api_url=api_url,
        repository=repository,
        token=token,
        pr_number=pr_number,
        base_sha=base_sha,
        head_sha=head_sha,
    )
    run_rust, reason = classify_changed_files(files)
    write_output(output, run_rust, reason, len(files))
    print(
        f"Rust CodeQL impact: run_rust={str(run_rust).lower()} "
        f"changed_files={len(files)} reason={reason}"
    )
    return 0


def self_test() -> int:
    positive = [
        "crates/oxide-batch/src/lib.rs",
        "xtask/src/main.rs",
        "Cargo.toml",
        "crates/oxide-batch/Cargo.toml",
        "Cargo.lock",
        ".cargo/config.toml",
        "rust-toolchain.toml",
        ".github/workflows/codeql.yml",
        ".github/scripts/codeql-rust-impact.py",
        ".github/workflows/dependency-review.yml",
        ".github/scripts/validate_actions_security.py",
        ".github/scripts/test_validate_actions_security.py",
        ".github/codeql-config.yml",
        ".github/codeql/rust.yml",
    ]
    for path in positive:
        if not rust_impact_path(path):
            raise AssertionError(f"expected Rust impact for {path}")

    negative = [
        "README.md",
        "docs/engineering/codeql-capability-review.md",
        ".github/workflows/ci.yml",
        ".github/workflows/m5-conformance.yml",
        "tests/fixtures/conformance/campaign-semantics.json",
        "migrations/0001.sql",
    ]
    for path in negative:
        if rust_impact_path(path):
            raise AssertionError(f"unexpected Rust impact for {path}")

    run_rust, reason = classify_changed_files(
        [ChangedFile("README.md", "modified"), ChangedFile("docs/x.md", "added")]
    )
    if run_rust or reason != "no-rust-impact":
        raise AssertionError("docs-only classification must skip Rust CodeQL")

    run_rust, reason = classify_changed_files(
        [ChangedFile("docs/new.md", "renamed", "crates/old.rs")]
    )
    if not run_rust or reason != "rust-impact:crates/old.rs":
        raise AssertionError("renaming away a Rust source must retain Rust impact")

    try:
        classify_changed_files([ChangedFile("README.md", "mystery")])
    except ClassificationError:
        pass
    else:
        raise AssertionError("unknown changed-file status must fail closed")

    try:
        rust_impact_path("bad\npath.rs")
    except ClassificationError:
        pass
    else:
        raise AssertionError("newline-bearing changed path must fail closed")

    print("CodeQL Rust impact classifier self-test: PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    return runtime()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ClassificationError, KeyError, ValueError, OSError, urllib.error.URLError) as exc:
        print(f"CodeQL Rust impact classification failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2)
