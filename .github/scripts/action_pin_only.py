#!/usr/bin/env python3
"""Fail-closed, opt-in classifier for action SHA patch updates.

This module is NOT an active merge authority until protected-base integration is
reviewed and staged. Call only with trusted-base text and candidate text.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable

# Audited step locations are an explicit trust boundary. Job layout changes
# require separate protected-base approval and a renewed site inventory.
ALLOWED_SITES: dict[str, dict[int, str]] = {
    ".github/workflows/pr-ci.yml": {
        34: "actions/checkout",
        130: "actions/checkout",
        191: "actions/checkout",
    },
    ".github/workflows/ci.yml": {
        144: "actions/checkout",
        240: "actions/checkout",
        294: "actions/cache/restore",
        337: "actions/checkout",
        391: "actions/cache/restore",
        434: "actions/checkout",
        488: "actions/cache/restore",
        531: "actions/checkout",
        585: "actions/cache/restore",
        628: "actions/checkout",
        650: "actions/checkout",
        658: "actions/checkout",
        712: "actions/checkout",
        757: "actions/checkout",
        775: "actions/checkout",
        802: "actions/checkout",
        844: "actions/checkout",
        1077: "actions/checkout",
        1131: "actions/cache/restore",
        1265: "actions/checkout",
    },
    ".github/workflows/dependency-review.yml": {
        48: "actions/dependency-review-action",
        71: "actions/checkout",
    },
    ".github/workflows/codeql.yml": {
        63: "actions/checkout",
        94: "actions/checkout",
        134: "actions/checkout",
        142: "github/codeql-action/init",
        149: "github/codeql-action/analyze",
        322: "actions/checkout",
        329: "github/codeql-action/init",
        335: "github/codeql-action/analyze",
    },
    ".github/workflows/evidence.yml": {
        64: "actions/checkout",
    },
    ".github/workflows/supply-chain.yml": {
        48: "actions/checkout",
    },
    ".github/workflows/m5-conformance.yml": {
        31: "actions/checkout",
        115: "actions/checkout",
        131: "actions/upload-artifact",
        164: "actions/checkout",
        180: "actions/upload-artifact",
        196: "actions/checkout",
        205: "actions/download-artifact",
        216: "actions/upload-artifact",
        232: "actions/checkout",
        241: "actions/download-artifact",
        252: "actions/upload-artifact",
    },
}

ACTION = re.compile(
    r"^(?P<indent>[ ]+)uses: (?P<action>[a-z0-9_.-]+/[a-z0-9_.-]+(?:/[a-z0-9_.-]+)?)"
    r"@(?P<sha>[0-9a-f]{40}) # v(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)\.(?P<patch>0|[1-9][0-9]*)"
    r"(?P<newline>\n?)$"
)


class AdmissionError(ValueError):
    """Candidate must be blocked; do not silently fall back to allow."""


@dataclass(frozen=True)
class Patch:
    repository: str
    old_sha: str
    new_sha: str
    old_version: tuple[int, int, int]
    new_version: tuple[int, int, int]


def _parse_line(line: str, expected_action: str) -> re.Match[str]:
    match = ACTION.fullmatch(line)
    if not match or match.group("action") != expected_action:
        raise AdmissionError("changed uses line is malformed or action identity changed")
    return match


def classify(path: str, trusted: bytes, candidate: bytes) -> tuple[Patch, ...]:
    """Allow unchanged raw bytes except audited step Action SHA and patch comment.

    Every returned patch also requires independent upstream release verification
    before any caller can accept the candidate.
    """
    sites = ALLOWED_SITES.get(path)
    if not sites:
        raise AdmissionError("workflow is not allowed for pin-only updates")
    try:
        before = trusted.decode("utf-8").splitlines(keepends=True)
        after = candidate.decode("utf-8").splitlines(keepends=True)
    except UnicodeError as exc:
        raise AdmissionError("workflow is not valid UTF-8") from exc
    if len(before) != len(after) or not before or before == after:
        raise AdmissionError("line count changed or no pin update was made")

    updates: dict[str, Patch] = {}
    changed = 0
    for number, (old_line, new_line) in enumerate(zip(before, after), start=1):
        if old_line == new_line:
            continue
        action = sites.get(number)
        if action is None:
            raise AdmissionError(f"non-allowlisted workflow change at line {number}")
        old = _parse_line(old_line, action)
        new = _parse_line(new_line, action)
        if old.group("indent") != new.group("indent") or old.group("newline") != new.group("newline"):
            raise AdmissionError("uses indentation/newline changed")
        old_ver = tuple(int(old.group(part)) for part in ("major", "minor", "patch"))
        new_ver = tuple(int(new.group(part)) for part in ("major", "minor", "patch"))
        if old_ver[:2] != new_ver[:2] or new_ver[2] <= old_ver[2]:
            raise AdmissionError("only forward patch releases on the same minor line are allowed")
        if old.group("sha") == new.group("sha"):
            raise AdmissionError("version comment changed without a new immutable SHA")
        repository = "/".join(action.split("/")[:2])
        patch = Patch(repository, old.group("sha"), new.group("sha"), old_ver, new_ver)
        if repository in updates and updates[repository] != patch:
            raise AdmissionError("inconsistent action repository pin update across workflow")
        updates[repository] = patch
        changed += 1

    for number, action in sites.items():
        if number > len(before):
            raise AdmissionError("audited site is absent")
        old = _parse_line(before[number - 1], action)
        repository = "/".join(action.split("/")[:2])
        if repository in updates:
            patch = updates[repository]
            new = _parse_line(after[number - 1], action)
            if old.group("sha") != patch.old_sha or new.group("sha") != patch.new_sha:
                raise AdmissionError("partial or mixed pin changes are not allowed")
    if changed == 0:
        raise AdmissionError("no approved pin update")
    return tuple(updates[key] for key in sorted(updates))


def verify_release(patch: Patch, fetch_json: Callable[[str], object]) -> None:
    """Require an official GitHub Release tag to resolve to the pinned commit."""
    version = ".".join(str(x) for x in patch.new_version)
    repository = patch.repository
    release = fetch_json(f"repos/{repository}/releases/tags/v{version}")
    if not isinstance(release, dict) or release.get("draft") is not False or release.get("prerelease") is not False or release.get("tag_name") != f"v{version}":
        raise AdmissionError("official versioned GitHub Release could not be verified")
    ref = fetch_json(f"repos/{repository}/git/ref/tags/v{version}")
    if not isinstance(ref, dict) or ref.get("ref") != f"refs/tags/v{version}":
        raise AdmissionError("tag ref is missing or inconsistent")
    object_ref = ref.get("object")
    for _ in range(3):
        if not isinstance(object_ref, dict):
            raise AdmissionError("malformed upstream Git tag object")
        kind, sha = object_ref.get("type"), object_ref.get("sha")
        if kind == "commit":
            if sha != patch.new_sha:
                raise AdmissionError("release tag does not point to the pinned commit")
            return
        if kind != "tag" or not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise AdmissionError("unknown Git tag reference type")
        data = fetch_json(f"repos/{repository}/git/tags/{sha}")
        if not isinstance(data, dict):
            raise AdmissionError("malformed annotated tag")
        object_ref = data.get("object")
    raise AdmissionError("nested release tag depth exceeded")


def github_api(token: str) -> Callable[[str], object]:
    """Read upstream release metadata; any API failure is fail-closed."""
    def read(relative: str) -> object:
        req = urllib.request.Request(
            "https://api.github.com/" + urllib.parse.quote(relative, safe="/"),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.load(response)
        except (OSError, UnicodeError, ValueError) as exc:
            raise AdmissionError("GitHub upstream release metadata unavailable") from exc
    return read


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-closed action pin-only gate")
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--trusted-file", required=True)
    parser.add_argument("--candidate-file", required=True)
    args = parser.parse_args()
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("pin-only: FAIL: missing GitHub API token", file=sys.stderr)
        return 1
    try:
        with open(args.trusted_file, "rb") as f:
            trusted = f.read()
        with open(args.candidate_file, "rb") as f:
            candidate = f.read()
        patches = classify(args.workflow, trusted, candidate)
        get_json = github_api(token)
        for patch in patches:
            verify_release(patch, get_json)
    except (OSError, AdmissionError) as exc:
        print(f"pin-only: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({
        "classification": "patch-only",
        "workflow": args.workflow,
        "verified": len(patches),
        "releases": [
            {"repository": p.repository, "old_sha": p.old_sha, "new_sha": p.new_sha,
             "new_version": ".".join(map(str, p.new_version))}
            for p in patches
        ],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
