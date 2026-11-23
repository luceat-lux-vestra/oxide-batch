#!/usr/bin/env python3
"""Validate EG-DEP-001 ownership metadata for fixed external tools and images."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA = "oxide-batch/dependency-update-policy/v1"
POLICY = "EG-DEP-001"
ALLOWED_STRATEGIES = {"DEPENDABOT", "CHANNEL", "UPDATE_PR", "REVIEWED_FREEZE"}
REQUIRED_TEXT_FIELDS = {
    "id",
    "source",
    "strategy",
    "authority",
    "mechanism",
    "owner",
    "validation",
    "provenance",
}
REVISIT_FIELDS = {"review_by", "rationale", "evidence", "trigger"}
MAX_FREEZE_DAYS = 90


def _non_empty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _safe_relative_path(value: str) -> bool:
    path = PurePosixPath(value)
    return not path.is_absolute() and ".." not in path.parts and value not in {"", "."}


def validate_document(document: Any, root: Path, today: dt.date) -> list[str]:
    errors: list[str] = []
    if not isinstance(document, dict):
        return ["registry root must be a JSON object"]

    if document.get("schema") != SCHEMA:
        errors.append(f"schema must be {SCHEMA!r}")
    if document.get("policy") != POLICY:
        errors.append(f"policy must be {POLICY!r}")
    if not _non_empty_text(document.get("owner")):
        errors.append("owner must be a non-empty string")

    reviewed_raw = document.get("reviewed_at")
    try:
        reviewed_at = dt.date.fromisoformat(reviewed_raw) if _non_empty_text(reviewed_raw) else None
    except ValueError:
        reviewed_at = None
    if reviewed_at is None:
        errors.append("reviewed_at must be an ISO date")

    entries = document.get("entries")
    if not isinstance(entries, list) or not entries:
        errors.append("entries must be a non-empty list")
        return errors

    seen: set[str] = set()
    for index, entry in enumerate(entries):
        prefix = f"entries[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{prefix} must be an object")
            continue

        for field in sorted(REQUIRED_TEXT_FIELDS):
            if not _non_empty_text(entry.get(field)):
                errors.append(f"{prefix}.{field} must be a non-empty string")

        entry_id = entry.get("id")
        if _non_empty_text(entry_id):
            if entry_id in seen:
                errors.append(f"{prefix}.id duplicates {entry_id!r}")
            seen.add(entry_id)

        strategy = entry.get("strategy")
        if strategy not in ALLOWED_STRATEGIES:
            errors.append(f"{prefix}.strategy must be one of {sorted(ALLOWED_STRATEGIES)}")

        source_paths = entry.get("source_paths")
        if not isinstance(source_paths, list) or not source_paths:
            errors.append(f"{prefix}.source_paths must be a non-empty list")
        else:
            local_seen: set[str] = set()
            for path_index, source_path in enumerate(source_paths):
                path_prefix = f"{prefix}.source_paths[{path_index}]"
                if not _non_empty_text(source_path) or not _safe_relative_path(source_path):
                    errors.append(f"{path_prefix} must be a safe relative repository path")
                    continue
                if source_path in local_seen:
                    errors.append(f"{path_prefix} duplicates {source_path!r}")
                    continue
                local_seen.add(source_path)
                if not (root / source_path).is_file():
                    errors.append(f"{path_prefix} does not exist: {source_path}")

        revisit = entry.get("revisit")
        if strategy == "REVIEWED_FREEZE":
            if not isinstance(revisit, dict):
                errors.append(f"{prefix}.revisit is required for REVIEWED_FREEZE")
                continue
            for field in sorted(REVISIT_FIELDS):
                if not _non_empty_text(revisit.get(field)):
                    errors.append(f"{prefix}.revisit.{field} must be a non-empty string")
            review_by_raw = revisit.get("review_by")
            try:
                review_by = (
                    dt.date.fromisoformat(review_by_raw)
                    if _non_empty_text(review_by_raw)
                    else None
                )
            except ValueError:
                review_by = None
            if review_by is None:
                errors.append(f"{prefix}.revisit.review_by must be an ISO date")
            else:
                if review_by < today:
                    errors.append(
                        f"{prefix}.revisit.review_by expired on {review_by.isoformat()}"
                    )
                if reviewed_at is not None:
                    interval = (review_by - reviewed_at).days
                    if interval < 0:
                        errors.append(
                            f"{prefix}.revisit.review_by precedes reviewed_at"
                        )
                    elif interval > MAX_FREEZE_DAYS:
                        errors.append(
                            f"{prefix}.revisit review window is {interval} days; "
                            f"maximum is {MAX_FREEZE_DAYS}"
                        )
        elif revisit not in (None, {}):
            errors.append(f"{prefix}.revisit is only valid for REVIEWED_FREEZE")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--registry",
        default=".github/dependency-update-policy.json",
        help="repository-relative policy registry",
    )
    parser.add_argument(
        "--root",
        default=".",
        help="repository root used to validate source_paths",
    )
    parser.add_argument(
        "--today",
        help="ISO date override used by deterministic tests",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    registry_path = root / args.registry
    try:
        document = json.loads(registry_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        print(f"dependency-update policy registry could not be read: {error}")
        return 1

    try:
        today = dt.date.fromisoformat(args.today) if args.today else dt.datetime.now(dt.timezone.utc).date()
    except ValueError as error:
        print(f"invalid --today value: {error}")
        return 1

    errors = validate_document(document, root, today)
    if errors:
        for error in errors:
            print(f"dependency-update-policy: {error}")
        return 1

    print(
        f"dependency-update-policy: PASS ({len(document['entries'])} entries, "
        f"reviewed_at={document['reviewed_at']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
