#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.request

MARKER = "<!-- oxide-batch:hardening-drift-audit -->"
TITLE = "ci: repository hardening drift audit requires attention"
LABELS = ["type:task", "area:governance", "area:ci", "priority:p1"]


class GitHubClient:
    def __init__(self, repository: str, token: str):
        self.base = f"https://api.github.com/repos/{repository}"
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "oxide-batch-hardening-drift-reporter",
        }

    def request(self, method: str, url: str, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(url, data=data, headers=self.headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"GitHub API {method} {url} failed: {exc.code}: {body}"
            ) from exc

    def all_issues(self):
        page = 1
        while True:
            items = self.request(
                "GET", f"{self.base}/issues?state=all&per_page=100&page={page}"
            )
            if not items:
                return
            for item in items:
                if "pull_request" not in item:
                    yield item
            if len(items) < 100:
                return
            page += 1

    def create_issue(self, title, body, labels):
        return self.request(
            "POST", f"{self.base}/issues", {"title": title, "body": body, "labels": labels}
        )

    def update_issue(self, number, **payload):
        return self.request("PATCH", f"{self.base}/issues/{number}", payload)

    def comment(self, number, body):
        return self.request(
            "POST", f"{self.base}/issues/{number}/comments", {"body": body}
        )


def find_owned_issue(client):
    matches = [issue for issue in client.all_issues() if MARKER in (issue.get("body") or "")]
    if len(matches) > 1:
        raise RuntimeError(
            f"multiple owned hardening drift issues found: {[issue['number'] for issue in matches]}"
        )
    return matches[0] if matches else None


def render_body(audit, run_url):
    return "\n".join([
        MARKER,
        "## Repository hardening drift audit",
        "",
        f"**Classification:** `{audit['classification']}`",
        "",
        f"**Workflow run:** {run_url}",
        "",
        "### Confirmed policy drift",
        "",
        "```json",
        json.dumps(audit.get("policy_findings", []), indent=2)[:24000],
        "```",
        "",
        "### Infrastructure/readback failures",
        "",
        "```json",
        json.dumps(audit.get("infrastructure_failures", []), indent=2)[:12000],
        "```",
        "",
        "### Explicit manual-readback controls",
        "",
        "These controls remain canonical but are not falsely claimed as continuously monitored by the low-privilege scheduled job.",
        "",
        "```json",
        json.dumps(audit.get("manual_readback", []), indent=2)[:16000],
        "```",
        "",
        "Repeated non-clean runs update/reopen this one owned issue. A later clean run records recovery and closes it.",
        "",
    ])


def reconcile_issue(client, audit, run_url):
    classification = audit.get("classification")
    if classification not in {"clean", "policy-drift", "infrastructure-failure"}:
        raise ValueError(f"unsupported audit classification: {classification!r}")

    owned = find_owned_issue(client)
    if classification == "clean":
        if owned and owned.get("state") == "open":
            client.comment(owned["number"], f"Audit recovered to clean. Run: {run_url}")
            client.update_issue(owned["number"], state="closed", state_reason="completed")
            return f"closed recovered issue #{owned['number']}"
        return "clean; no open owned issue"

    body = render_body(audit, run_url)
    if owned is None:
        created = client.create_issue(TITLE, body, LABELS)
        return f"created issue #{created['number']}"

    payload = {"title": TITLE, "body": body, "labels": LABELS}
    if owned.get("state") != "open":
        payload.update({"state": "open", "state_reason": "reopened"})
    client.update_issue(owned["number"], **payload)
    client.comment(
        owned["number"],
        f"Audit remains non-clean with classification `{classification}`. Run: {run_url}",
    )
    return f"updated issue #{owned['number']}"


def load_result(args):
    if args.result is not None:
        return json.loads(args.result.read_text(encoding="utf-8"))
    return json.loads(args.result_json)


def main():
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--result", type=Path)
    source.add_argument("--result-json")
    parser.add_argument("--run-url", required=True)
    args = parser.parse_args()

    repository = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    if not repository or not token:
        print("GITHUB_REPOSITORY and GITHUB_TOKEN are required", file=sys.stderr)
        return 2

    try:
        audit = load_result(args)
        print(reconcile_issue(GitHubClient(repository, token), audit, args.run_url))
        return 0
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError, TypeError) as exc:
        print(f"hardening drift reporting failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
