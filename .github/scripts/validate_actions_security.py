#!/usr/bin/env python3
"""Fail-closed static policy checks for GitHub Actions workflow security."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
IMAGE_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
USES_LINE = re.compile(r"^(?P<indent>\s*)(?P<dash>-\s*)?uses:\s*(?P<value>[^\s#]+)")
IMAGE_LINE = re.compile(r"^(?P<indent>\s*)image:\s*(?P<value>[^\s#]+)")
PROGRAM_LINE = re.compile(
    r"^(?P<indent>\s*)(?P<dash>-\s*)?(?P<key>run|script):\s*(?P<value>.*)$"
)
EXPRESSION = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")

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
                if not IMAGE_DIGEST.search(image):
                    violations.append(
                        f"{path}:{index + 1}: docker action must be digest-pinned: {value}"
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
            value = image.group("value")
            if not IMAGE_DIGEST.search(value):
                violations.append(
                    f"{path}:{index + 1}: service/container image must retain a readable tag and immutable sha256 digest: {value}"
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="repository root containing .github/workflows",
    )
    args = parser.parse_args()

    violations = validate(args.root.resolve())
    if violations:
        print("GitHub Actions security policy violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1

    print("GitHub Actions deterministic security policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
