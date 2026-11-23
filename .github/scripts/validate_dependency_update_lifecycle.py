#!/usr/bin/env python3
"""Validate EG-DEP-001 lifecycle ownership and finite reviewed freezes."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ALLOWED = {"DEPENDABOT", "CHANNEL", "UPDATE_PR", "REVIEWED_FREEZE"}
SHA256 = re.compile(r"^[0-9a-f]{64}$")
MAX_FREEZE_DAYS = 90


def parse_date(value: object, *, label: str, errors: list[str]) -> dt.date | None:
    try:
        return dt.date.fromisoformat(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        errors.append(f"{label}: invalid ISO date")
        return None


def validate(data: dict, *, today: dt.date) -> list[str]:
    errors: list[str] = []
    if data.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if data.get("policy") != "EG-DEP-001":
        errors.append("policy must be EG-DEP-001")
    timezone = data.get("timezone")
    if not isinstance(timezone, str) or not timezone:
        errors.append("timezone must be a non-empty IANA timezone")

    reviewed_at = parse_date(data.get("reviewed_at"), label="reviewed_at", errors=errors)
    if reviewed_at is not None and reviewed_at > today:
        errors.append("reviewed_at cannot be in the future")

    entries = data.get("entries")
    if not isinstance(entries, list) or not entries:
        return errors + ["entries must be a non-empty array"]

    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            errors.append("every entry must be an object")
            continue
        entry_id = entry.get("id")
        if not isinstance(entry_id, str) or not entry_id:
            errors.append("entry id must be a non-empty string")
            continue
        if entry_id in seen:
            errors.append(f"{entry_id}: duplicate id")
        seen.add(entry_id)

        for field in ("source", "strategy", "authority", "mechanism", "validation", "revisit_trigger"):
            if not entry.get(field):
                errors.append(f"{entry_id}: missing {field}")
        strategy = entry.get("strategy")
        if strategy not in ALLOWED:
            errors.append(f"{entry_id}: unsupported strategy {strategy!r}")
        if not isinstance(entry.get("validation"), list) or not entry["validation"]:
            errors.append(f"{entry_id}: validation must be a non-empty array")

        if strategy == "REVIEWED_FREEZE":
            for field in ("owner", "reason", "review_by"):
                if not entry.get(field):
                    errors.append(f"{entry_id}: reviewed freeze missing {field}")
            review_by = parse_date(
                entry.get("review_by"),
                label=f"{entry_id}: review_by",
                errors=errors,
            )
            if review_by is not None:
                if review_by <= today:
                    errors.append(f"{entry_id}: reviewed freeze is due/expired ({review_by})")
                if reviewed_at is not None and review_by > reviewed_at + dt.timedelta(days=MAX_FREEZE_DAYS):
                    errors.append(
                        f"{entry_id}: reviewed freeze exceeds {MAX_FREEZE_DAYS}-day review interval"
                    )

        pin = entry.get("pin")
        if pin is not None:
            if not isinstance(pin, dict) or not pin.get("repository") or not pin.get("version"):
                errors.append(f"{entry_id}: invalid pin metadata")
                continue
            artifacts = pin.get("artifacts")
            if not isinstance(artifacts, dict) or not artifacts:
                errors.append(f"{entry_id}: pin artifacts must be non-empty")
                continue
            for target, artifact in artifacts.items():
                digest = artifact.get("sha256") if isinstance(artifact, dict) else None
                if not isinstance(target, str) or not SHA256.fullmatch(digest or ""):
                    errors.append(f"{entry_id}: invalid artifact digest for {target!r}")
    return errors


def policy_today(data: dict) -> dt.date:
    timezone = data.get("timezone")
    if not isinstance(timezone, str) or not timezone:
        raise ValueError("missing lifecycle timezone")
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown lifecycle timezone: {timezone}") from exc
    return dt.datetime.now(zone).date()


def self_test(data: dict) -> None:
    today = policy_today(data)
    if validate(data, today=today):
        raise RuntimeError("current lifecycle registry failed validation")

    frozen = next(item for item in data["entries"] if item["strategy"] == "REVIEWED_FREEZE")

    expired = copy.deepcopy(data)
    next(item for item in expired["entries"] if item["id"] == frozen["id"])["review_by"] = (
        today - dt.timedelta(days=1)
    ).isoformat()
    if not validate(expired, today=today):
        raise RuntimeError("expired freeze mutation was not rejected")

    missing_reason = copy.deepcopy(data)
    next(item for item in missing_reason["entries"] if item["id"] == frozen["id"]).pop("reason", None)
    if not validate(missing_reason, today=today):
        raise RuntimeError("freeze without rationale was not rejected")

    excessive = copy.deepcopy(data)
    reviewed_at = dt.date.fromisoformat(excessive["reviewed_at"])
    next(item for item in excessive["entries"] if item["id"] == frozen["id"])["review_by"] = (
        reviewed_at + dt.timedelta(days=MAX_FREEZE_DAYS + 1)
    ).isoformat()
    if not validate(excessive, today=today):
        raise RuntimeError("overlong freeze mutation was not rejected")

    bad_digest = copy.deepcopy(data)
    pinned = next(item for item in bad_digest["entries"] if item.get("pin"))
    next(iter(pinned["pin"]["artifacts"].values()))["sha256"] = "not-a-digest"
    if not validate(bad_digest, today=today):
        raise RuntimeError("invalid artifact digest mutation was not rejected")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument(
        "--registry",
        default=".github/dependency-update-lifecycle.json",
    )
    args = parser.parse_args()

    path = Path(args.registry)
    data = json.loads(path.read_text(encoding="utf-8"))
    if args.self_test:
        self_test(data)

    try:
        today = policy_today(data)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    errors = validate(data, today=today)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("PASS: EG-DEP-001 lifecycle ownership and reviewed-freeze deadlines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
