#!/usr/bin/env python3
"""Validate that every cargo-deny policy exception is owned and time-bounded."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
import tomllib

BASELINE_REGISTRIES = {"https://github.com/rust-lang/crates.io-index"}
KINDS = {"advisory", "license", "ban", "source"}


def _target(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def deny_exceptions(config: dict) -> set[tuple[str, str]]:
    """Return only policy waivers/relaxations, not restrictive policy entries."""
    found: set[tuple[str, str]] = set()

    for advisory in config.get("advisories", {}).get("ignore", []):
        found.add(("advisory", _target(advisory)))

    for exception in config.get("licenses", {}).get("exceptions", []):
        found.add(("license", _target(exception)))

    bans = config.get("bans", {})
    for field in ("skip", "skip-tree"):
        for entry in bans.get(field, []):
            found.add(("ban", f"{field}:{_target(entry)}"))
    for entry in bans.get("build", {}).get("bypass", []):
        found.add(("ban", f"build-bypass:{_target(entry)}"))

    sources = config.get("sources", {})
    for registry in set(sources.get("allow-registry", [])) - BASELINE_REGISTRIES:
        found.add(("source", f"registry:{registry}"))
    for git_source in sources.get("allow-git", []):
        found.add(("source", f"git:{_target(git_source)}"))
    allow_org = sources.get("allow-org", {})
    if isinstance(allow_org, dict):
        for provider, organizations in allow_org.items():
            for organization in organizations or []:
                found.add(("source", f"org:{provider}:{_target(organization)}"))

    return found


def registry_exceptions(registry: dict, today: dt.date) -> tuple[set[tuple[str, str]], list[str]]:
    violations: list[str] = []
    if registry.get("schema_version") != 1:
        violations.append("exception registry schema_version must be 1")

    entries = registry.get("exceptions")
    if not isinstance(entries, list):
        return set(), violations + ["exception registry must contain an exceptions array"]

    found: set[tuple[str, str]] = set()
    for index, entry in enumerate(entries):
        prefix = f"exception #{index}"
        if not isinstance(entry, dict):
            violations.append(f"{prefix} must be an object")
            continue

        values: dict[str, str] = {}
        for field in ("kind", "target", "owner", "rationale", "expires"):
            value = entry.get(field)
            if not isinstance(value, str) or not value.strip():
                violations.append(f"{prefix} is missing non-empty {field}")
            else:
                values[field] = value.strip()

        kind = values.get("kind")
        if kind is not None and kind not in KINDS:
            violations.append(f"{prefix} kind must be one of {sorted(KINDS)}")

        expires = values.get("expires")
        if expires is not None:
            try:
                expiry = dt.date.fromisoformat(expires)
            except ValueError:
                violations.append(f"{prefix} expires must be a real YYYY-MM-DD date")
            else:
                if expiry < today:
                    violations.append(f"{prefix} expired on {expiry.isoformat()}")

        target = values.get("target")
        if kind in KINDS and target is not None:
            key = (kind, target)
            if key in found:
                violations.append(f"{prefix} duplicates registry entry {kind}:{target}")
            found.add(key)

    return found, violations


def validate(deny: dict, registry: dict, today: dt.date) -> list[str]:
    actual = deny_exceptions(deny)
    declared, violations = registry_exceptions(registry, today)

    for kind, target in sorted(actual - declared):
        violations.append(
            f"deny.toml exception {kind}:{target} has no owner/rationale/expiry registry entry"
        )
    for kind, target in sorted(declared - actual):
        violations.append(
            f"registry entry {kind}:{target} does not correspond to an active deny.toml exception"
        )
    return violations


def load(path: Path) -> object:
    if path.suffix == ".toml":
        with path.open("rb") as handle:
            return tomllib.load(handle)
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deny", type=Path, default=Path("deny.toml"))
    parser.add_argument(
        "--registry", type=Path, default=Path(".github/supply-chain-exceptions.json")
    )
    parser.add_argument("--today", type=dt.date.fromisoformat, default=dt.date.today())
    args = parser.parse_args()

    violations = validate(load(args.deny), load(args.registry), args.today)
    for violation in violations:
        print(f"supply-chain exception violation: {violation}", file=sys.stderr)
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
