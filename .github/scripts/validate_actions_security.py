#!/usr/bin/env python3
"""Fail-closed static policy checks for GitHub Actions workflow security."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
READABLE_DIGEST_IMAGE = re.compile(r"^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$")
USES_LINE = re.compile(r"^(?P<indent>\s*)(?P<dash>-\s*)?uses:\s*(?P<value>[^\s#]+)")
IMAGE_LINE = re.compile(r"^(?P<indent>\s*)image:\s*(?P<value>.+)$")
PROGRAM_LINE = re.compile(
    r"^(?P<indent>\s*)(?P<dash>-\s*)?(?P<key>run|script):\s*(?P<value>.*)$"
)
EXPRESSION = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")
POSTGRES_15_DIGEST = "926f8799aef36e00001cfe15fba7abbd37d3c5224ea57e4c858e4bb670f10561"
POSTGRES_18_DIGEST = "4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
POSTGRES_MATRIX_IMAGE = re.compile(
    r"^postgres:\$\{\{\s*matrix\.postgres\s*\}\}@sha256:\$\{\{\s*"
    r"matrix\.postgres\s*==\s*'15'\s*&&\s*'(?P<pg15>[0-9a-f]{64})'\s*\|\|\s*"
    r"matrix\.postgres\s*==\s*'18'\s*&&\s*'(?P<pg18>[0-9a-f]{64})'\s*\|\|\s*"
    r"'unsupported'\s*\}\}$"
)
POSTGRES_MATRIX = re.compile(
    r'^\s*postgres:\s*\[\s*"15"\s*,\s*"18"\s*\]\s*(?:#.*)?
UNTRUSTED_PREFIXES = (
    "github.event.pull_request.",
    "github.event.issue.",
    "github.event.comment.",
    "github.event.review.",
    "github.event.review_comment.",
    "github.event.release.",
    "github.event.workflow_run.",
    "github.event.inputs.",
    "inputs.",
)
UNTRUSTED_EXACT = {
    "github.head_ref",
    "github.ref",
    "github.ref_name",
}


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _is_untrusted_expression(expression: str) -> bool:
    normalized = re.sub(r"\s+", "", expression)
    # Expressions can contain operators; treat any occurrence of the risky
    # context as untrusted instead of requiring the whole expression to match.
    if any(prefix in normalized for prefix in UNTRUSTED_PREFIXES):
        return True
    return any(
        re.search(rf"(?<![A-Za-z0-9_.]){re.escape(name)}(?![A-Za-z0-9_.])", normalized)
        for name in UNTRUSTED_EXACT
    )


def _program_block(lines: list[str], index: int) -> tuple[list[tuple[int, str]], int]:
    """Return program text lines for run:/script: at index and last consumed index."""
    match = PROGRAM_LINE.match(lines[index])
    if match is None:
        return [], index
    base_indent = len(match.group("indent"))
    value = match.group("value").strip()
    if value and not value.startswith(("|", ">")):
        return [(index + 1, value)], index

    body: list[tuple[int, str]] = []
    cursor = index + 1
    while cursor < len(lines):
        line = lines[cursor]
        if not line.strip():
            body.append((cursor + 1, line))
            cursor += 1
            continue
        if _indent(line) <= base_indent:
            break
        body.append((cursor + 1, line))
        cursor += 1
    return body, cursor - 1


def _checkout_step_has_persist_false(
    lines: list[str], uses_index: int, match: re.Match[str]
) -> bool:
    uses_indent = len(match.group("indent"))
    step_indent = uses_indent if match.group("dash") else max(0, uses_indent - 2)

    cursor = uses_index + 1
    while cursor < len(lines):
        line = lines[cursor]
        stripped = line.lstrip()
        if line.strip():
            indent = _indent(line)
            if indent < step_indent:
                break
            if indent == step_indent and stripped.startswith("- "):
                break
        if re.match(r"^\s+persist-credentials:\s*false\s*(?:#.*)?$", line):
            return True
        cursor += 1
    return False


def _strip_yaml_comment(value: str) -> str:
    return re.sub(r"\s+#.*$", "", value).strip()


def _is_allowed_image(lines: list[str], value: str) -> bool:
    if READABLE_DIGEST_IMAGE.fullmatch(value):
        return True

    matrix_image = POSTGRES_MATRIX_IMAGE.fullmatch(value)
    if matrix_image is None:
        return False
    if not any(POSTGRES_MATRIX.fullmatch(line) for line in lines):
        return False
    return (
        matrix_image.group("pg15") == POSTGRES_15_DIGEST
        and matrix_image.group("pg18") == POSTGRES_18_DIGEST
    )


def check_ephemeral_workflow_lifecycle(path: Path, text: str) -> list[str]:
    """Require explicit ownership and retirement conditions for ephemeral workflows."""
    if not EPHEMERAL_WORKFLOW.fullmatch(path.name):
        return []

    header = text.splitlines()[:20]
    owner = next(
        (match.group(1).strip() for line in header if (match := LIFECYCLE_OWNER.match(line))),
        "",
    )
    retire = next(
        (match.group(1).strip() for line in header if (match := LIFECYCLE_RETIRE.match(line))),
        "",
    )

    violations: list[str] = []
    if not owner:
        violations.append(
            f"{path}: ephemeral workflow requires non-empty # lifecycle-owner: header metadata"
        )
    if not retire:
        violations.append(
            f"{path}: ephemeral workflow requires non-empty # lifecycle-retire: header metadata"
        )
    return violations


def check_workflow(path: Path, text: str) -> list[str]:
    lines = text.splitlines()
    violations = check_ephemeral_workflow_lifecycle(path, text)

    pull_request_target = any(
        re.match(r"^\s*pull_request_target\s*:", line)
        or re.match(r"^on:\s*pull_request_target\s*(?:#.*)?$", line)
        for line in lines
    )

    for index, line in enumerate(lines):
        uses = USES_LINE.match(line)
        if uses:
            value = uses.group("value")
            if value.startswith("./"):
                pass
            elif value.startswith("docker://"):
                image = value.removeprefix("docker://")
                if not READABLE_DIGEST_IMAGE.fullmatch(image):
                    violations.append(
                        f"{path}:{index + 1}: docker action must retain a readable tag and immutable sha256 digest: {value}"
                    )
            elif "@" not in value:
                violations.append(
                    f"{path}:{index + 1}: external uses reference must include @<40-hex-SHA>: {value}"
                )
            else:
                ref = value.rsplit("@", 1)[1]
                if not FULL_SHA.fullmatch(ref):
                    violations.append(
                        f"{path}:{index + 1}: external uses reference must use a full 40-hex commit SHA: {value}"
                    )

            if value.startswith("actions/checkout@"):
                if not _checkout_step_has_persist_false(lines, index, uses):
                    violations.append(
                        f"{path}:{index + 1}: actions/checkout must set persist-credentials: false"
                    )
                if pull_request_target:
                    violations.append(
                        f"{path}:{index + 1}: pull_request_target workflow must not checkout repository code"
                    )

        image = IMAGE_LINE.match(line)
        if image:
            value = _strip_yaml_comment(image.group("value"))
            if not _is_allowed_image(lines, value):
                violations.append(
                    f"{path}:{index + 1}: service/container image must retain a readable tag and immutable sha256 digest, or use the exact checked-in PostgreSQL 15/18 digest mapping: {value}"
                )

        if re.match(r"^\s*permissions:\s*write-all\s*(?:#.*)?$", line):
            violations.append(f"{path}:{index + 1}: permissions: write-all is forbidden")

        # Workflow-top-level write permissions apply to every job and are too
        # broad. Read-only top-level defaults are fine; writes belong on the
        # narrowest job that needs them.
        if re.match(r"^permissions:\s*(?:#.*)?$", line):
            cursor = index + 1
            while cursor < len(lines):
                child = lines[cursor]
                if child.strip() and _indent(child) == 0:
                    break
                if re.match(r"^\s{2}[A-Za-z0-9_-]+:\s*write\s*(?:#.*)?$", child):
                    violations.append(
                        f"{path}:{cursor + 1}: workflow-top-level write permission is forbidden; grant it at job scope"
                    )
                cursor += 1

        program = PROGRAM_LINE.match(line)
        if program:
            body, _ = _program_block(lines, index)
            for lineno, program_line in body:
                for expression in EXPRESSION.findall(program_line):
                    if _is_untrusted_expression(expression):
                        violations.append(
                            f"{path}:{lineno}: untrusted GitHub/input context must cross into program text via env/with data, not direct template interpolation: ${{{{ {expression.strip()} }}}}"
                        )

    return violations


def workflow_paths(root: Path) -> list[Path]:
    workflows = root / ".github" / "workflows"
    return sorted(
        path
        for path in workflows.iterdir()
        if path.is_file() and path.suffix in {".yml", ".yaml"}
    )


def validate(root: Path) -> list[str]:
    violations: list[str] = []
    for path in workflow_paths(root):
        violations.extend(check_workflow(path.relative_to(root), path.read_text()))
    return violations


def check_issue_labeler_contract_text(
    text: str, path: Path = Path(".github/workflows/issue-labeler.yml")
) -> list[str]:
    """Validate the repository-specific mutation boundary for backlog reconciliation."""
    violations: list[str] = []
    required_fragments = (
        "workflow_dispatch:",
        "dry_run:",
        "backfill:",
        "DRY_RUN:",
        "BACKFILL:",
        "if (!dryRun)",
        "name !== explicitType && !dryRun",
        "if (!dryRun && uniqueAdd.length)",
        "const defaultBranchRef = `refs/heads/${context.payload.repository.default_branch}`;",
        "context.eventName === 'workflow_dispatch' && backfill && !dryRun && context.ref !== defaultBranchRef",
        "Mutating backfill must run from",
    )
    for fragment in required_fragments:
        if fragment not in text:
            violations.append(f"{path}: issue reconciliation safety contract missing: {fragment}")

    if not re.search(r"(?ms)^      dry_run:\n.*?^        default: true\s*$", text):
        violations.append(f"{path}: dry_run must default to true")
    if not re.search(r"(?ms)^      backfill:\n.*?^        default: false\s*$", text):
        violations.append(f"{path}: backfill must default to false")

    if not re.search(r"(?m)^permissions:\s*\{\}\s*$", text):
        violations.append(f"{path}: workflow-level permissions must remain empty")

    writes = re.findall(r"(?m)^\s+([A-Za-z0-9_-]+):\s*write\s*(?:#.*)?$", text)
    if writes != ["issues"]:
        violations.append(
            f"{path}: write authority must be exactly one job-local issues:write grant; got {writes!r}"
        )

    expected_mutations = {
        "github.rest.issues.updateLabel": 1,
        "github.rest.issues.createLabel": 1,
        "github.rest.issues.removeLabel": 1,
        "github.rest.issues.addLabels": 1,
    }
    for call, expected in expected_mutations.items():
        observed = text.count(call)
        if observed != expected:
            violations.append(
                f"{path}: mutation surface drifted for {call}: expected {expected}, got {observed}"
            )
    return violations


def check_issue_labeler_contract(root: Path) -> list[str]:
    path = root / ".github" / "workflows" / "issue-labeler.yml"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"{path}: cannot read issue labeler: {exc}"]
    return check_issue_labeler_contract_text(text, path.relative_to(root))


def _job_block(text: str, job_id: str) -> str | None:
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        text,
    )
    return match.group(0) if match else None


def check_codeql_weekly_manual_contract_text(
    text: str, path: Path = Path(".github/workflows/codeql.yml")
) -> list[str]:
    """Require exact-main full scans and reject PR-derived CodeQL invocation."""
    violations: list[str] = []
    event_match = re.search(
        r"(?ms)^on:\n(?P<body>.*?)(?=^permissions:\n)",
        text,
    )
    if event_match is None:
        return [f"{path}: CodeQL must declare schedule/manual event block"]
    event_block = event_match.group("body")
    events = re.findall(r"(?m)^  ([a-z_]+):", event_block)
    if events != ["schedule", "workflow_dispatch"]:
        violations.append(
            f"{path}: scheduled-only CodeQL must have exactly schedule and workflow_dispatch events"
        )
    for fragment in (
        'cron: "47 18 * * 1"',
        "inputs:",
        "candidate_sha:",
        "required: true",
    ):
        if fragment not in event_block:
            violations.append(f"{path}: full scan event contract missing: {fragment}")
    for forbidden in ("pull_request:", "workflow_run:", "workflow_call:", "push:"):
        if forbidden in event_block:
            violations.append(f"{path}: PR/push-derived CodeQL event forbidden: {forbidden}")

    verify = _job_block(text, "verify-source")
    if verify is None:
        violations.append(f"{path}: trusted exact-source verification job missing")
    else:
        for fragment in (
            "name: Verify trusted CodeQL source identity",
            "runs-on: ubuntu-slim",
            "GH_TOKEN:",
            "INPUT_SHA:",
            'test "$GITHUB_REF" = "refs/heads/main"',
            '[[ "$GITHUB_SHA" =~ ^[0-9a-f]{40}$ ]]',
            '[[ "$INPUT_SHA" =~ ^[0-9a-f]{40}$ ]]',
            'test "$INPUT_SHA" = "$GITHUB_SHA"',
            'test "$live_sha" = "$INPUT_SHA"',
            'echo "candidate_sha=$GITHUB_SHA"',
            'case "$GITHUB_EVENT_NAME" in',
            "workflow_dispatch)",
            "schedule)",
        ):
            if fragment not in verify:
                violations.append(f"{path}: exact source guard missing: {fragment}")
        api_path = "repos/" + "$" + "{GITHUB_REPOSITORY}/git/ref/heads/main"
        if api_path not in verify or ("\\" + api_path) in verify:
            violations.append(f"{path}: live protected main API URL malformed or escaped")
        if "candidate_sha: ${{ steps.check.outputs.candidate_sha }}" not in verify:
            violations.append(f"{path}: scanned SHA output is not bound")

    if _job_block(text, "rust-impact") is not None:
        violations.append(f"{path}: obsolete PR Rust-impact routing remains")
    if _job_block(text, "orphan-codeql-live") is not None:
        violations.append(f"{path}: obsolete PR orphan cancellation remains")

    for job_id, language in (("analyze-actions", "actions"), ("analyze-rust", "rust")):
        job = _job_block(text, job_id)
        if job is None:
            violations.append(f"{path}: full CodeQL {language} analysis job missing")
            continue
        for fragment in (
            f"name: Analyze ({language})",
            "needs: verify-source",
            "security-events: write",
            "needs.verify-source.outputs.candidate_sha",
            'test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"',
            "github/codeql-action/init@",
            "github/codeql-action/analyze@",
            f"languages: {language}",
            "build-mode: none",
            f"category: /language:{language}",
        ):
            if fragment not in job:
                violations.append(
                    f"{path}: full CodeQL {language} analysis contract missing: {fragment}"
                )
        if any(token in job for token in ("pull_request", "head_repo", "run_rust=false")):
            violations.append(
                f"{path}: full CodeQL {language} must not use PR source or conditional skip"
            )
    return violations


def check_codeql_weekly_manual_contract(root: Path) -> list[str]:
    path = root / ".github" / "workflows" / "codeql.yml"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"{path}: cannot read CodeQL workflow: {exc}"]
    return check_codeql_weekly_manual_contract_text(text, path.relative_to(root))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="repository root containing .github/workflows",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    violations = validate(root)
    violations.extend(check_issue_labeler_contract(root))
    violations.extend(check_codeql_weekly_manual_contract(root))
    if violations:
        print("GitHub Actions security policy violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1

    print("GitHub Actions deterministic security policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

)

EPHEMERAL_WORKFLOW = re.compile(
    r"^(?:research|temporary|tmp|experiment)(?:[-_].*)?\.ya?ml$",
    re.IGNORECASE,
)
LIFECYCLE_OWNER = re.compile(
    r"^\s*#\s*lifecycle-owner:\s*(\S.*)$",
    re.IGNORECASE,
)
LIFECYCLE_RETIRE = re.compile(
    r"^\s*#\s*lifecycle-retire:\s*(\S.*)$",
    re.IGNORECASE,
)

UNTRUSTED_PREFIXES = (
    "github.event.pull_request.",
    "github.event.issue.",
    "github.event.comment.",
    "github.event.review.",
    "github.event.review_comment.",
    "github.event.release.",
    "github.event.workflow_run.",
    "github.event.inputs.",
    "inputs.",
)
UNTRUSTED_EXACT = {
    "github.head_ref",
    "github.ref",
    "github.ref_name",
}


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _is_untrusted_expression(expression: str) -> bool:
    normalized = re.sub(r"\s+", "", expression)
    # Expressions can contain operators; treat any occurrence of the risky
    # context as untrusted instead of requiring the whole expression to match.
    if any(prefix in normalized for prefix in UNTRUSTED_PREFIXES):
        return True
    return any(
        re.search(rf"(?<![A-Za-z0-9_.]){re.escape(name)}(?![A-Za-z0-9_.])", normalized)
        for name in UNTRUSTED_EXACT
    )


def _program_block(lines: list[str], index: int) -> tuple[list[tuple[int, str]], int]:
    """Return program text lines for run:/script: at index and last consumed index."""
    match = PROGRAM_LINE.match(lines[index])
    if match is None:
        return [], index
    base_indent = len(match.group("indent"))
    value = match.group("value").strip()
    if value and not value.startswith(("|", ">")):
        return [(index + 1, value)], index

    body: list[tuple[int, str]] = []
    cursor = index + 1
    while cursor < len(lines):
        line = lines[cursor]
        if not line.strip():
            body.append((cursor + 1, line))
            cursor += 1
            continue
        if _indent(line) <= base_indent:
            break
        body.append((cursor + 1, line))
        cursor += 1
    return body, cursor - 1


def _checkout_step_has_persist_false(
    lines: list[str], uses_index: int, match: re.Match[str]
) -> bool:
    uses_indent = len(match.group("indent"))
    step_indent = uses_indent if match.group("dash") else max(0, uses_indent - 2)

    cursor = uses_index + 1
    while cursor < len(lines):
        line = lines[cursor]
        stripped = line.lstrip()
        if line.strip():
            indent = _indent(line)
            if indent < step_indent:
                break
            if indent == step_indent and stripped.startswith("- "):
                break
        if re.match(r"^\s+persist-credentials:\s*false\s*(?:#.*)?$", line):
            return True
        cursor += 1
    return False


def _strip_yaml_comment(value: str) -> str:
    return re.sub(r"\s+#.*$", "", value).strip()


def _is_allowed_image(lines: list[str], value: str) -> bool:
    if READABLE_DIGEST_IMAGE.fullmatch(value):
        return True

    matrix_image = POSTGRES_MATRIX_IMAGE.fullmatch(value)
    if matrix_image is None:
        return False
    if not any(POSTGRES_MATRIX.fullmatch(line) for line in lines):
        return False
    return (
        matrix_image.group("pg15") == POSTGRES_15_DIGEST
        and matrix_image.group("pg18") == POSTGRES_18_DIGEST
    )


def check_workflow(path: Path, text: str) -> list[str]:
    lines = text.splitlines()
    violations: list[str] = []

    pull_request_target = any(
        re.match(r"^\s*pull_request_target\s*:", line)
        or re.match(r"^on:\s*pull_request_target\s*(?:#.*)?$", line)
        for line in lines
    )

    for index, line in enumerate(lines):
        uses = USES_LINE.match(line)
        if uses:
            value = uses.group("value")
            if value.startswith("./"):
                pass
            elif value.startswith("docker://"):
                image = value.removeprefix("docker://")
                if not READABLE_DIGEST_IMAGE.fullmatch(image):
                    violations.append(
                        f"{path}:{index + 1}: docker action must retain a readable tag and immutable sha256 digest: {value}"
                    )
            elif "@" not in value:
                violations.append(
                    f"{path}:{index + 1}: external uses reference must include @<40-hex-SHA>: {value}"
                )
            else:
                ref = value.rsplit("@", 1)[1]
                if not FULL_SHA.fullmatch(ref):
                    violations.append(
                        f"{path}:{index + 1}: external uses reference must use a full 40-hex commit SHA: {value}"
                    )

            if value.startswith("actions/checkout@"):
                if not _checkout_step_has_persist_false(lines, index, uses):
                    violations.append(
                        f"{path}:{index + 1}: actions/checkout must set persist-credentials: false"
                    )
                if pull_request_target:
                    violations.append(
                        f"{path}:{index + 1}: pull_request_target workflow must not checkout repository code"
                    )

        image = IMAGE_LINE.match(line)
        if image:
            value = _strip_yaml_comment(image.group("value"))
            if not _is_allowed_image(lines, value):
                violations.append(
                    f"{path}:{index + 1}: service/container image must retain a readable tag and immutable sha256 digest, or use the exact checked-in PostgreSQL 15/18 digest mapping: {value}"
                )

        if re.match(r"^\s*permissions:\s*write-all\s*(?:#.*)?$", line):
            violations.append(f"{path}:{index + 1}: permissions: write-all is forbidden")

        # Workflow-top-level write permissions apply to every job and are too
        # broad. Read-only top-level defaults are fine; writes belong on the
        # narrowest job that needs them.
        if re.match(r"^permissions:\s*(?:#.*)?$", line):
            cursor = index + 1
            while cursor < len(lines):
                child = lines[cursor]
                if child.strip() and _indent(child) == 0:
                    break
                if re.match(r"^\s{2}[A-Za-z0-9_-]+:\s*write\s*(?:#.*)?$", child):
                    violations.append(
                        f"{path}:{cursor + 1}: workflow-top-level write permission is forbidden; grant it at job scope"
                    )
                cursor += 1

        program = PROGRAM_LINE.match(line)
        if program:
            body, _ = _program_block(lines, index)
            for lineno, program_line in body:
                for expression in EXPRESSION.findall(program_line):
                    if _is_untrusted_expression(expression):
                        violations.append(
                            f"{path}:{lineno}: untrusted GitHub/input context must cross into program text via env/with data, not direct template interpolation: ${{{{ {expression.strip()} }}}}"
                        )

    return violations


def workflow_paths(root: Path) -> list[Path]:
    workflows = root / ".github" / "workflows"
    return sorted(
        path
        for path in workflows.iterdir()
        if path.is_file() and path.suffix in {".yml", ".yaml"}
    )


def validate(root: Path) -> list[str]:
    violations: list[str] = []
    for path in workflow_paths(root):
        violations.extend(check_workflow(path.relative_to(root), path.read_text()))
    return violations


def check_issue_labeler_contract_text(
    text: str, path: Path = Path(".github/workflows/issue-labeler.yml")
) -> list[str]:
    """Validate the repository-specific mutation boundary for backlog reconciliation."""
    violations: list[str] = []
    required_fragments = (
        "workflow_dispatch:",
        "dry_run:",
        "backfill:",
        "DRY_RUN:",
        "BACKFILL:",
        "if (!dryRun)",
        "name !== explicitType && !dryRun",
        "if (!dryRun && uniqueAdd.length)",
        "const defaultBranchRef = `refs/heads/${context.payload.repository.default_branch}`;",
        "context.eventName === 'workflow_dispatch' && backfill && !dryRun && context.ref !== defaultBranchRef",
        "Mutating backfill must run from",
    )
    for fragment in required_fragments:
        if fragment not in text:
            violations.append(f"{path}: issue reconciliation safety contract missing: {fragment}")

    if not re.search(r"(?ms)^      dry_run:\n.*?^        default: true\s*$", text):
        violations.append(f"{path}: dry_run must default to true")
    if not re.search(r"(?ms)^      backfill:\n.*?^        default: false\s*$", text):
        violations.append(f"{path}: backfill must default to false")

    if not re.search(r"(?m)^permissions:\s*\{\}\s*$", text):
        violations.append(f"{path}: workflow-level permissions must remain empty")

    writes = re.findall(r"(?m)^\s+([A-Za-z0-9_-]+):\s*write\s*(?:#.*)?$", text)
    if writes != ["issues"]:
        violations.append(
            f"{path}: write authority must be exactly one job-local issues:write grant; got {writes!r}"
        )

    expected_mutations = {
        "github.rest.issues.updateLabel": 1,
        "github.rest.issues.createLabel": 1,
        "github.rest.issues.removeLabel": 1,
        "github.rest.issues.addLabels": 1,
    }
    for call, expected in expected_mutations.items():
        observed = text.count(call)
        if observed != expected:
            violations.append(
                f"{path}: mutation surface drifted for {call}: expected {expected}, got {observed}"
            )
    return violations


def check_issue_labeler_contract(root: Path) -> list[str]:
    path = root / ".github" / "workflows" / "issue-labeler.yml"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"{path}: cannot read issue labeler: {exc}"]
    return check_issue_labeler_contract_text(text, path.relative_to(root))


def _job_block(text: str, job_id: str) -> str | None:
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        text,
    )
    return match.group(0) if match else None


def check_codeql_weekly_manual_contract_text(
    text: str, path: Path = Path(".github/workflows/codeql.yml")
) -> list[str]:
    """Require exact-main full scans and reject PR-derived CodeQL invocation."""
    violations: list[str] = []
    event_match = re.search(
        r"(?ms)^on:\n(?P<body>.*?)(?=^permissions:\n)",
        text,
    )
    if event_match is None:
        return [f"{path}: CodeQL must declare schedule/manual event block"]
    event_block = event_match.group("body")
    events = re.findall(r"(?m)^  ([a-z_]+):", event_block)
    if events != ["schedule", "workflow_dispatch"]:
        violations.append(
            f"{path}: scheduled-only CodeQL must have exactly schedule and workflow_dispatch events"
        )
    for fragment in (
        'cron: "47 18 * * 1"',
        "inputs:",
        "candidate_sha:",
        "required: true",
    ):
        if fragment not in event_block:
            violations.append(f"{path}: full scan event contract missing: {fragment}")
    for forbidden in ("pull_request:", "workflow_run:", "workflow_call:", "push:"):
        if forbidden in event_block:
            violations.append(f"{path}: PR/push-derived CodeQL event forbidden: {forbidden}")

    verify = _job_block(text, "verify-source")
    if verify is None:
        violations.append(f"{path}: trusted exact-source verification job missing")
    else:
        for fragment in (
            "name: Verify trusted CodeQL source identity",
            "runs-on: ubuntu-slim",
            "GH_TOKEN:",
            "INPUT_SHA:",
            'test "$GITHUB_REF" = "refs/heads/main"',
            '[[ "$GITHUB_SHA" =~ ^[0-9a-f]{40}$ ]]',
            '[[ "$INPUT_SHA" =~ ^[0-9a-f]{40}$ ]]',
            'test "$INPUT_SHA" = "$GITHUB_SHA"',
            'test "$live_sha" = "$INPUT_SHA"',
            'echo "candidate_sha=$GITHUB_SHA"',
            'case "$GITHUB_EVENT_NAME" in',
            "workflow_dispatch)",
            "schedule)",
        ):
            if fragment not in verify:
                violations.append(f"{path}: exact source guard missing: {fragment}")
        api_path = "repos/" + "$" + "{GITHUB_REPOSITORY}/git/ref/heads/main"
        if api_path not in verify or ("\\" + api_path) in verify:
            violations.append(f"{path}: live protected main API URL malformed or escaped")
        if "candidate_sha: ${{ steps.check.outputs.candidate_sha }}" not in verify:
            violations.append(f"{path}: scanned SHA output is not bound")

    if _job_block(text, "rust-impact") is not None:
        violations.append(f"{path}: obsolete PR Rust-impact routing remains")
    if _job_block(text, "orphan-codeql-live") is not None:
        violations.append(f"{path}: obsolete PR orphan cancellation remains")

    for job_id, language in (("analyze-actions", "actions"), ("analyze-rust", "rust")):
        job = _job_block(text, job_id)
        if job is None:
            violations.append(f"{path}: full CodeQL {language} analysis job missing")
            continue
        for fragment in (
            f"name: Analyze ({language})",
            "needs: verify-source",
            "security-events: write",
            "needs.verify-source.outputs.candidate_sha",
            'test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"',
            "github/codeql-action/init@",
            "github/codeql-action/analyze@",
            f"languages: {language}",
            "build-mode: none",
            f"category: /language:{language}",
        ):
            if fragment not in job:
                violations.append(
                    f"{path}: full CodeQL {language} analysis contract missing: {fragment}"
                )
        if any(token in job for token in ("pull_request", "head_repo", "run_rust=false")):
            violations.append(
                f"{path}: full CodeQL {language} must not use PR source or conditional skip"
            )
    return violations


def check_codeql_weekly_manual_contract(root: Path) -> list[str]:
    path = root / ".github" / "workflows" / "codeql.yml"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"{path}: cannot read CodeQL workflow: {exc}"]
    return check_codeql_weekly_manual_contract_text(text, path.relative_to(root))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="repository root containing .github/workflows",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    violations = validate(root)
    violations.extend(check_issue_labeler_contract(root))
    violations.extend(check_codeql_weekly_manual_contract(root))
    if violations:
        print("GitHub Actions security policy violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1

    print("GitHub Actions deterministic security policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
