#!/usr/bin/env python3
"""Validate and resolve EG-DEP-001 ownership metadata for fixed external pins."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
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
}
REVISIT_FIELDS = {"review_by", "rationale", "evidence", "trigger"}
MAX_FREEZE_DAYS = 90
SHA256 = re.compile(r"^[0-9a-f]{64}$")
DIGEST_IMAGE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")


def _non_empty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _safe_relative_path(value: str) -> bool:
    path = PurePosixPath(value)
    return not path.is_absolute() and ".." not in path.parts and value not in {"", "."}


def _validate_provenance(value: Any, prefix: str, errors: list[str]) -> None:
    if not isinstance(value, dict):
        errors.append(f"{prefix}.provenance must be an object")
        return

    artifacts = value.get("artifacts")
    images = value.get("images")
    if artifacts is None and images is None:
        errors.append(f"{prefix}.provenance must declare artifacts or images")

    if artifacts is not None:
        if not _non_empty_text(value.get("version")):
            errors.append(f"{prefix}.provenance.version must be a non-empty string")
        if not isinstance(artifacts, dict) or not artifacts:
            errors.append(f"{prefix}.provenance.artifacts must be a non-empty object")
        else:
            for name, artifact in artifacts.items():
                artifact_prefix = f"{prefix}.provenance.artifacts[{name!r}]"
                if not _non_empty_text(name):
                    errors.append(f"{artifact_prefix} key must be non-empty")
                if not isinstance(artifact, dict):
                    errors.append(f"{artifact_prefix} must be an object")
                    continue
                url = artifact.get("url")
                sha256 = artifact.get("sha256")
                if not _non_empty_text(url) or not url.startswith("https://"):
                    errors.append(f"{artifact_prefix}.url must be an https URL")
                if not _non_empty_text(sha256) or SHA256.fullmatch(sha256) is None:
                    errors.append(f"{artifact_prefix}.sha256 must be 64 lowercase hex characters")

    if images is not None:
        if not isinstance(images, dict) or not images:
            errors.append(f"{prefix}.provenance.images must be a non-empty object")
        else:
            for name, image in images.items():
                image_prefix = f"{prefix}.provenance.images[{name!r}]"
                if not _non_empty_text(name):
                    errors.append(f"{image_prefix} key must be non-empty")
                if not _non_empty_text(image) or DIGEST_IMAGE.fullmatch(image) is None:
                    errors.append(f"{image_prefix} must be a tag plus immutable sha256 digest")

    product_versions = value.get("product_versions")
    if product_versions is not None:
        if not isinstance(product_versions, dict) or not product_versions:
            errors.append(f"{prefix}.provenance.product_versions must be a non-empty object")
        else:
            for name, product_version in product_versions.items():
                if not _non_empty_text(name) or not _non_empty_text(product_version):
                    errors.append(
                        f"{prefix}.provenance.product_versions entries must be non-empty strings"
                    )


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
    elif reviewed_at > today:
        errors.append(f"reviewed_at cannot be in the future: {reviewed_at.isoformat()}")

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

        _validate_provenance(entry.get("provenance"), prefix, errors)

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
                        errors.append(f"{prefix}.revisit.review_by precedes reviewed_at")
                    elif interval > MAX_FREEZE_DAYS:
                        errors.append(
                            f"{prefix}.revisit review window is {interval} days; "
                            f"maximum is {MAX_FREEZE_DAYS}"
                        )
        elif revisit not in (None, {}):
            errors.append(f"{prefix}.revisit is only valid for REVIEWED_FREEZE")

    return errors


def resolve_value(
    document: dict[str, Any],
    entry_id: str,
    field: str,
    artifact: str | None = None,
    image: str | None = None,
) -> str:
    entries = document.get("entries")
    if not isinstance(entries, list):
        raise ValueError("registry entries are unavailable")
    matches = [entry for entry in entries if isinstance(entry, dict) and entry.get("id") == entry_id]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one entry {entry_id!r}, found {len(matches)}")
    provenance = matches[0].get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError(f"entry {entry_id!r} has no structured provenance")

    if field == "version":
        value = provenance.get("version")
    elif field in {"url", "sha256"}:
        if not artifact:
            raise ValueError(f"--artifact is required for field {field}")
        artifacts = provenance.get("artifacts")
        if not isinstance(artifacts, dict) or artifact not in artifacts:
            raise ValueError(f"entry {entry_id!r} has no artifact {artifact!r}")
        record = artifacts[artifact]
        value = record.get(field) if isinstance(record, dict) else None
    elif field == "image":
        if not image:
            raise ValueError("--image is required for field image")
        images = provenance.get("images")
        value = images.get(image) if isinstance(images, dict) else None
    else:
        raise ValueError(f"unsupported field {field!r}")

    if not _non_empty_text(value):
        raise ValueError(
            f"entry {entry_id!r} does not resolve a non-empty {field!r} value"
        )
    return value


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
    parser.add_argument("--entry", help="resolve one entry after validation")
    parser.add_argument("--artifact", help="artifact key for url/sha256 resolution")
    parser.add_argument("--image", help="image key for image resolution")
    parser.add_argument(
        "--field",
        choices=("version", "url", "sha256", "image"),
        help="resolved provenance field to print",
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
        today = (
            dt.date.fromisoformat(args.today)
            if args.today
            else dt.datetime.now(dt.timezone.utc).date()
        )
    except ValueError as error:
        print(f"invalid --today value: {error}")
        return 1

    errors = validate_document(document, root, today)
    if errors:
        for error in errors:
            print(f"dependency-update-policy: {error}")
        return 1

    if args.entry or args.field or args.artifact or args.image:
        if not args.entry or not args.field:
            print("dependency-update-policy: --entry and --field are required for resolution")
            return 1
        try:
            print(resolve_value(document, args.entry, args.field, args.artifact, args.image))
        except ValueError as error:
            print(f"dependency-update-policy: {error}")
            return 1
        return 0

    print(
        f"dependency-update-policy: PASS ({len(document['entries'])} entries, "
        f"reviewed_at={document['reviewed_at']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
