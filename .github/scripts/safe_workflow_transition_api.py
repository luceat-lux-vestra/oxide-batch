#!/usr/bin/env python3
"""Read-only GitHub evidence collector for the NON-AUTHORITATIVE safe-diff prototype.

Run from a trusted default-branch checkout, never from PR-head source. The
caller obtains commit/tree/blob identities from distinct GitHub API endpoints;
it does not trust PR-authored check results or GitHub patch snippets.
No result returned by this module is sufficient to authorize a merge.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import re
import sys
import unittest
import urllib.error
import urllib.request

from safe_workflow_transition import (
    SHA, WORKFLOW, SafeWorkflowTransitionTests, TransitionDenied,
    evaluate, git_blob_id,
)

REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MAX_FILE_PAGES = 30
MAX_BODY = 1_048_576
MAX_BLOB = 131_072


def exact_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or SHA.fullmatch(value) is None:
        raise TransitionDenied(f"{label}: missing exact 40-hex SHA")
    return value


def record(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        raise TransitionDenied(f"{label}: invalid GitHub API object")
    return value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise TransitionDenied("GitHub API redirected an authenticated read")


class GitHubReadOnly:
    """Fixed-origin, bounded, read-only REST client. Token is never logged."""

    def __init__(self, repository: str, token: str):
        if not isinstance(repository, str) or REPOSITORY.fullmatch(repository) is None:
            raise TransitionDenied("invalid repository")
        if not isinstance(token, str) or not token:
            raise TransitionDenied("missing read-only GitHub API token")
        self.repository = repository
        self.token = token
        self.opener = urllib.request.build_opener(_NoRedirect())
        self.api_prefix = f"https://api.github.com/repos/{repository}/"

    def get(self, route: str):
        # Routes are constructed solely from exact validated SHAs, a trusted
        # constant workflow path, and bounded page numbers.
        if not isinstance(route, str) or not re.fullmatch(r"[A-Za-z0-9_./?=&-]+", route):
            raise TransitionDenied("invalid GitHub API route")
        request = urllib.request.Request(
            self.api_prefix + route,
            headers={
                "Authorization": "Bearer " + self.token,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "oxide-batch-safe-transition-advisory",
            },
        )
        try:
            with self.opener.open(request, timeout=15) as response:
                body = response.read(MAX_BODY + 1)
            if len(body) > MAX_BODY:
                raise TransitionDenied("GitHub API response exceeds size bound")
            return json.loads(body.decode("utf-8"))
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise TransitionDenied("GitHub API read failed or returned invalid JSON") from exc


def _live_pr(api, number: int) -> dict:
    return record(api.get(f"pulls/{number}"), "live pull request")


def _pr_identity(pr: dict) -> tuple:
    base = record(pr.get("base"), "PR base")
    head = record(pr.get("head"), "PR head")
    base_repo = record(base.get("repo"), "PR base repository")
    head_repo = record(head.get("repo"), "PR head repository")
    count = pr.get("changed_files")
    if type(pr.get("number")) is not int or type(count) is not int:
        raise TransitionDenied("missing PR number or changed-file count")
    if not 1 <= count <= 3000:
        raise TransitionDenied("unsupported or empty changed-file inventory")
    return (
        pr["number"], pr.get("state"), pr.get("draft"), pr.get("merged"),
        count, base.get("ref"), base.get("sha"), base_repo.get("full_name"),
        head.get("sha"), head_repo.get("full_name"),
    )


def _main_sha(api) -> str:
    branch = record(api.get("branches/main"), "main branch")
    return exact_sha(record(branch.get("commit"), "main branch commit").get("sha"), "main")


def _changed_files(api, number: int, expected_count: int) -> list[dict]:
    # Always paginate independently of any user-supplied list. GitHub caps the
    # pull-request files endpoint at 3000 files; ambiguous caps fail CLOSED.
    files = []
    seen = set()
    for page in range(1, MAX_FILE_PAGES + 1):
        batch = api.get(f"pulls/{number}/files?per_page=100&page={page}")
        if not isinstance(batch, list) or len(batch) > 100:
            raise TransitionDenied("malformed PR file page")
        for item in batch:
            obj = record(item, "changed file")
            filename = obj.get("filename")
            if not isinstance(filename, str) or not filename or filename in seen:
                raise TransitionDenied("missing or duplicate changed-file path")
            seen.add(filename)
            files.append(obj)
        if len(files) > expected_count:
            raise TransitionDenied("changed-file pagination exceeded live PR count")
        if len(batch) < 100:
            if len(files) != expected_count:
                raise TransitionDenied("changed-file pagination truncated")
            return files
    raise TransitionDenied("changed-file pagination cap is ambiguous")


def _tree_entry(api, sha: str, name: str, kind: str, mode: str) -> str:
    tree = record(api.get(f"git/trees/{sha}"), "git tree")
    if exact_sha(tree.get("sha"), "tree") != sha or tree.get("truncated") is not False:
        raise TransitionDenied("Git tree identity mismatch or incomplete tree")
    entries = tree.get("tree")
    if not isinstance(entries, list):
        raise TransitionDenied("invalid Git tree members")
    matches = [e for e in entries if isinstance(e, dict) and e.get("path") == name]
    if len(matches) != 1:
        raise TransitionDenied("Git tree path missing or duplicated")
    item = matches[0]
    if item.get("type") != kind or item.get("mode") != mode:
        raise TransitionDenied("Git tree entry type/mode changed")
    return exact_sha(item.get("sha"), "tree entry")


def _file_blob_sha(api, commit_sha: str) -> str:
    commit = record(api.get(f"git/commits/{commit_sha}"), "git commit")
    if exact_sha(commit.get("sha"), "commit") != commit_sha:
        raise TransitionDenied("Git commit response identity mismatch")
    sha = exact_sha(record(commit.get("tree"), "commit tree").get("sha"), "root tree")
    segments = WORKFLOW.split("/")
    for segment in segments[:-1]:
        sha = _tree_entry(api, sha, segment, "tree", "040000")
    return _tree_entry(api, sha, segments[-1], "blob", "100644")


def _blob_text(api, sha: str) -> str:
    blob = record(api.get(f"git/blobs/{sha}"), "Git blob")
    if exact_sha(blob.get("sha"), "Git blob") != sha or blob.get("encoding") != "base64":
        raise TransitionDenied("Git blob identity/encoding mismatch")
    content = blob.get("content")
    size = blob.get("size")
    if not isinstance(content, str) or type(size) is not int or not 0 <= size <= MAX_BLOB:
        raise TransitionDenied("Git blob missing or exceeds size bound")
    try:
        raw = base64.b64decode("".join(content.splitlines()), validate=True)
        value = raw.decode("utf-8")
    except (binascii.Error, UnicodeError, ValueError) as exc:
        raise TransitionDenied("Git blob is not canonical base64 UTF-8") from exc
    if len(raw) != size or git_blob_id(value) != sha:
        raise TransitionDenied("Git blob content does not match tree identity")
    return value


def inspect(api, *, number: int, trusted_base: str, expected_head: str) -> dict:
    """Read-only advisory eligibility; no check-run verification or merge approval."""
    if type(number) is not int or number <= 0:
        raise TransitionDenied("invalid PR number")
    if not isinstance(getattr(api, "repository", None), str) or (
        REPOSITORY.fullmatch(api.repository) is None
    ):
        raise TransitionDenied("invalid API repository")
    base = exact_sha(trusted_base, "expected base")
    head = exact_sha(expected_head, "expected head")
    if base == head:
        raise TransitionDenied("identical base/head SHA")
    before = _live_pr(api, number)
    identity = _pr_identity(before)
    if (
        identity[0] != number
        or identity[1] != "open"
        or identity[2] is not False
        or identity[3] is not False
        or identity[5] != "main"
        or identity[6] != base
        or identity[7] != api.repository
        or identity[8] != head
        or identity[9] != api.repository
        or _main_sha(api) != base
    ):
        raise TransitionDenied("PR identity not anchored to current protected main/head")
    files = _changed_files(api, number, identity[4])
    base_blob_sha = _file_blob_sha(api, base)
    head_blob_sha = _file_blob_sha(api, head)
    base_text = _blob_text(api, base_blob_sha)
    head_text = _blob_text(api, head_blob_sha)
    # The GitHub Files endpoint also provides a head blob identity.
    if len(files) == 1 and files[0].get("filename") == WORKFLOW:
        file_sha = files[0].get("sha")
        if not isinstance(file_sha, str) or file_sha != head_blob_sha:
            raise TransitionDenied("PR files blob differs from head Git tree")
    after = _live_pr(api, number)
    if _pr_identity(after) != identity or _main_sha(api) != base:
        raise TransitionDenied("PR or protected base changed during inspection")
    result = evaluate(
        repository=api.repository, pr=after,
        trusted_base_sha=base, expected_head_sha=head, files=files,
        trusted_base_blob=base_blob_sha, expected_head_blob=head_blob_sha,
        base_text=base_text, head_text=head_text,
    )
    result["evidence_origin"] = "READ_ONLY_GITHUB_GIT_TREE_AND_BLOB"
    result["api_identity_rechecked"] = True
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository")
    parser.add_argument("--pr", type=int)
    parser.add_argument("--trusted-base")
    parser.add_argument("--expected-head")
    args = parser.parse_args()
    try:
        if os.environ.get("GITHUB_REF") != "refs/heads/main":
            raise TransitionDenied("collector must execute from trusted main only")
        if os.environ.get("GITHUB_SHA") != args.trusted_base:
            raise TransitionDenied("trusted checkout SHA and declared base differ")
        api = GitHubReadOnly(args.repository, os.environ.get("GH_TOKEN", ""))
        output = inspect(
            api, number=args.pr, trusted_base=args.trusted_base,
            expected_head=args.expected_head,
        )
        print(json.dumps(output, sort_keys=True))
        return 0
    except TransitionDenied as exc:
        print(f"safe-transition advisory: DENIED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
