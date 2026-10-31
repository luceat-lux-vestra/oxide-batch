#!/usr/bin/env python3
"""Build a fail-closed execution plan for R2A #112 / oxide-batch #626."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import subprocess
from collections import Counter
from pathlib import Path

SHA40 = re.compile(r"^[0-9a-f]{40}$")
SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
DISPATCH_REL = Path(".github/research/626-dispatch.json")
WORKFLOW_REL = Path(".github/workflows/research-626-integration-shard-ab.yml")
HARNESS_DIR = Path(".github/research/626")


class PlanError(ValueError):
    pass


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def read_dispatch(path: Path) -> dict:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PlanError(f"cannot read dispatch JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError("dispatch JSON must be an object")
    expected = {"schema", "harness_base_sha", "target_sha", "variant", "pair_id", "order_position"}
    if set(raw) != expected:
        raise PlanError(f"dispatch keys mismatch: expected={sorted(expected)}, observed={sorted(raw)}")
    if raw["schema"] != "oxide-batch-integration-ab/v1":
        raise PlanError(f"unsupported dispatch schema: {raw['schema']!r}")
    for key in ("harness_base_sha", "target_sha"):
        if not isinstance(raw[key], str) or not SHA40.fullmatch(raw[key]):
            raise PlanError(f"{key} must be a lowercase 40-hex SHA")
    if raw["variant"] not in {"A", "B"}:
        raise PlanError("variant must be exactly 'A' or 'B'")
    if not isinstance(raw["pair_id"], str) or not SAFE_ID.fullmatch(raw["pair_id"]):
        raise PlanError("pair_id must match [A-Za-z0-9._-]{1,64}")
    if raw["order_position"] not in {1, 2}:
        raise PlanError("order_position must be integer 1 or 2")
    if not raw["pair_id"].endswith(("-AB", "-BA")):
        raise PlanError("pair_id must end with -AB or -BA to encode crossover order")
    expected_variant = "A" if raw["order_position"] == 1 else "B"
    reverse_variant = "B" if raw["order_position"] == 1 else "A"
    if raw["pair_id"].endswith("-AB") and raw["variant"] != expected_variant:
        raise PlanError("pair_id suffix -AB requires order 1=A, 2=B")
    if raw["pair_id"].endswith("-BA") and raw["variant"] != reverse_variant:
        raise PlanError("pair_id suffix -BA requires order 1=B, 2=A")
    return raw


def load_production_sharder(target_root: Path):
    module_path = target_root / ".github/scripts/run-integration-shard.py"
    if not module_path.is_file():
        raise PlanError(f"target source is missing production sharder: {module_path}")
    spec = importlib.util.spec_from_file_location("production_integration_sharder", module_path)
    if spec is None or spec.loader is None:
        raise PlanError("cannot load production integration sharder")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_harness_delta(harness_root: Path, base_sha: str, head_sha: str) -> list[str]:
    if git(harness_root, "merge-base", "--is-ancestor", base_sha, head_sha) != "":
        # git merge-base --is-ancestor reports only by return code; a nonzero return is
        # raised by git() before this branch is reached.
        pass
    changed = [
        line
        for line in git(harness_root, "diff", "--name-only", base_sha, head_sha).splitlines()
        if line
    ]
    if changed != [DISPATCH_REL.as_posix()]:
        raise PlanError(
            "dispatch commit may differ from harness base only by "
            f"{DISPATCH_REL.as_posix()!r}; observed={changed}"
        )
    return changed


def harness_logic(root: Path) -> tuple[dict[str, str], str]:
    if not (root / HARNESS_DIR).is_dir():
        raise PlanError(f"missing harness directory: {HARNESS_DIR}")
    tracked = [
        Path(line)
        for line in git(
            root,
            "ls-files",
            "--",
            HARNESS_DIR.as_posix(),
            WORKFLOW_REL.as_posix(),
        ).splitlines()
        if line
    ]
    files = sorted(path for path in tracked if path != DISPATCH_REL)
    if WORKFLOW_REL not in files:
        raise PlanError(f"workflow is not tracked: {WORKFLOW_REL}")
    if not any(path.is_relative_to(HARNESS_DIR) for path in files):
        raise PlanError(f"no tracked harness logic files under: {HARNESS_DIR}")
    hashes: dict[str, str] = {}
    digest = hashlib.sha256()
    for rel in sorted(files):
        path = root / rel
        if not path.is_file():
            raise PlanError(f"missing harness logic file: {rel}")
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        hashes[rel.as_posix()] = sha
        digest.update(rel.as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha.encode("ascii"))
        digest.update(b"\n")
    return hashes, digest.hexdigest()


def build_plan(harness_root: Path, target_root: Path, dispatch_path: Path, head_sha: str) -> dict:
    dispatch = read_dispatch(dispatch_path)
    validate_harness_delta(harness_root, dispatch["harness_base_sha"], head_sha)

    actual_target_sha = git(target_root, "rev-parse", "HEAD")
    if actual_target_sha != dispatch["target_sha"]:
        raise PlanError(
            f"target checkout mismatch: expected={dispatch['target_sha']} observed={actual_target_sha}"
        )
    try:
        subprocess.run(
            ["git", "-C", str(harness_root), "merge-base", "--is-ancestor", actual_target_sha, "origin/main"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise PlanError(
            f"target SHA {actual_target_sha} is not reachable from protected main history"
        ) from exc

    sharder = load_production_sharder(target_root)
    if getattr(sharder, "SHARD_COUNT", None) != 4:
        raise PlanError(f"expected production SHARD_COUNT=4, observed={getattr(sharder, 'SHARD_COUNT', None)!r}")

    old_cwd = Path.cwd()
    try:
        os.chdir(target_root)
        targets = sharder.load_targets()
        partitions = sharder.partition(targets)
    finally:
        os.chdir(old_cwd)

    if len(partitions) != 4:
        raise PlanError(f"production sharder returned {len(partitions)} partitions, expected 4")
    flattened = [name for part in partitions for name in part]
    counts = Counter(flattened)
    if set(flattened) != set(targets) or any(count != 1 for count in counts.values()):
        raise PlanError("production partition is not an exact one-to-one cover")
    if any(not part for part in partitions):
        raise PlanError("production partition contains an empty shard")

    target_digest = hashlib.sha256(("\n".join(targets) + "\n").encode("utf-8")).hexdigest()
    logic_hashes, logic_digest = harness_logic(harness_root)

    return {
        "schema": "oxide-batch-integration-ab-plan/v1",
        "variant": dispatch["variant"],
        "pair_id": dispatch["pair_id"],
        "order_position": dispatch["order_position"],
        "harness_base_sha": dispatch["harness_base_sha"],
        "harness_dispatch_sha": head_sha,
        "harness_logic_sha256": logic_digest,
        "harness_logic_files": logic_hashes,
        "target_sha": actual_target_sha,
        "target_count": len(targets),
        "target_digest_sha256": target_digest,
        "targets": targets,
        "partitions": partitions,
        "partition_sizes": [len(part) for part in partitions],
    }


def write_outputs(plan: dict, output_path: Path | None) -> None:
    if output_path is None:
        return
    lines = {
        "variant": plan["variant"],
        "pair_id": plan["pair_id"],
        "order_position": str(plan["order_position"]),
        "target_sha": plan["target_sha"],
        "harness_base_sha": plan["harness_base_sha"],
        "target_count": str(plan["target_count"]),
        "target_digest": plan["target_digest_sha256"],
        "harness_logic_digest": plan["harness_logic_sha256"],
    }
    with output_path.open("a", encoding="utf-8") as handle:
        for key, value in lines.items():
            handle.write(f"{key}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harness-root", type=Path, required=True)
    parser.add_argument("--target-root", type=Path, required=True)
    parser.add_argument("--dispatch", type=Path, required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    try:
        plan = build_plan(
            args.harness_root.resolve(),
            args.target_root.resolve(),
            args.dispatch.resolve(),
            args.head_sha,
        )
    except (PlanError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"::error::{exc}")
        raise SystemExit(1) from exc

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_outputs(plan, args.github_output)
    print(
        "research plan ok: "
        f"variant={plan['variant']} pair={plan['pair_id']} "
        f"target={plan['target_sha']} targets={plan['target_count']} "
        f"partition_sizes={plan['partition_sizes']}"
    )


if __name__ == "__main__":
    main()
