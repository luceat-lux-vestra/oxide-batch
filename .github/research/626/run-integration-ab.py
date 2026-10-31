#!/usr/bin/env python3
"""Execute exactly one #626 research variant and retain non-rerun evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


class RunError(ValueError):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def command_output(command: list[str], *, cwd: Path | None = None) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def load_production_sharder(target_root: Path):
    module_path = target_root / ".github/scripts/run-integration-shard.py"
    spec = importlib.util.spec_from_file_location("production_integration_sharder_runtime", module_path)
    if spec is None or spec.loader is None:
        raise RunError(f"cannot load production sharder from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_environment(target_root: Path, plan: dict) -> dict:
    target_sha = command_output(["git", "rev-parse", "HEAD"], cwd=target_root)
    if target_sha != plan["target_sha"]:
        raise RunError(f"target SHA mismatch: expected={plan['target_sha']} observed={target_sha}")

    expected_env = {
        "RUNNER_ARCH": "ARM64",
        "SCCACHE_LOCAL_RW_MODE": "READ_ONLY",
        "RUSTC_WRAPPER": "sccache",
        "CARGO_INCREMENTAL": "0",
    }
    for name, expected in expected_env.items():
        observed = os.environ.get(name)
        if observed != expected:
            raise RunError(f"{name} mismatch: expected={expected!r} observed={observed!r}")

    machine = platform.machine()
    if machine != "aarch64":
        raise RunError(f"runner machine must be aarch64, observed={machine!r}")

    rustc = command_output(["rustc", "--version"])
    cargo = command_output(["cargo", "--version"])
    sccache_version = command_output(["sccache", "--version"])
    if not rustc.startswith("rustc 1.97.1 "):
        raise RunError(f"expected Rust 1.97.1, observed={rustc!r}")
    if "0.18.0" not in sccache_version:
        raise RunError(f"expected sccache 0.18.0, observed={sccache_version!r}")

    return {
        "target_sha": target_sha,
        "runner_arch": os.environ["RUNNER_ARCH"],
        "machine": machine,
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_name": os.environ.get("RUNNER_NAME"),
        "rustc": rustc,
        "cargo": cargo,
        "sccache": sccache_version,
        "sccache_local_rw_mode": os.environ["SCCACHE_LOCAL_RW_MODE"],
        "cargo_incremental": os.environ["CARGO_INCREMENTAL"],
        "cache_key": os.environ.get("RESEARCH_CACHE_KEY"),
        "cache_hit": os.environ.get("RESEARCH_CACHE_HIT"),
    }


def load_plan(path: Path) -> dict:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunError(f"cannot read plan: {exc}") from exc
    if plan.get("schema") != "oxide-batch-integration-ab-plan/v1":
        raise RunError("unexpected plan schema")
    return plan


def current_targets(target_root: Path, sharder) -> tuple[list[str], list[list[str]]]:
    old_cwd = Path.cwd()
    try:
        os.chdir(target_root)
        targets = sharder.load_targets()
        partitions = sharder.partition(targets)
    finally:
        os.chdir(old_cwd)
    return targets, partitions


def target_digest(targets: list[str]) -> str:
    return hashlib.sha256(("\n".join(targets) + "\n").encode("utf-8")).hexdigest()


def parse_sccache_stats() -> dict:
    raw = command_output(["sccache", "--show-stats", "--stats-format=json"])
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RunError(f"sccache emitted invalid JSON stats: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RunError("sccache stats JSON must be an object")
    return parsed


def execute(target_root: Path, plan: dict, variant: str, shard_index: int | None) -> tuple[list[str], list[str], int, float, dict]:
    sharder = load_production_sharder(target_root)
    targets, partitions = current_targets(target_root, sharder)
    if targets != plan["targets"]:
        raise RunError("live target inventory differs from the fail-closed plan")
    if target_digest(targets) != plan["target_digest_sha256"]:
        raise RunError("live target inventory digest differs from the plan")
    if partitions != plan["partitions"]:
        raise RunError("live production partition differs from the plan")

    if variant == "A":
        if shard_index is None or shard_index not in range(4):
            raise RunError("variant A requires --shard-index 0..3")
        selected = partitions[shard_index]
        command = [
            sys.executable,
            ".github/scripts/run-integration-shard.py",
            str(shard_index),
            "4",
        ]
    elif variant == "B":
        if shard_index is not None:
            raise RunError("variant B must not set --shard-index")
        selected = targets
        command = ["cargo", "test", "--workspace", "--all-features"]
        for name in selected:
            command.extend(["--test", name])
    else:
        raise RunError(f"unsupported variant: {variant!r}")

    subprocess.run(["sccache", "--zero-stats"], cwd=target_root, check=True)
    started = time.monotonic()
    completed = subprocess.run(command, cwd=target_root, check=False)
    elapsed = time.monotonic() - started
    stats = parse_sccache_stats()
    return targets, selected, completed.returncode, elapsed, stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=["A", "B"], required=True)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--target-root", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    evidence: dict = {
        "schema": "oxide-batch-integration-ab-evidence/v1",
        "variant": args.variant,
        "shard_index": args.shard_index,
        "status": "harness-error",
        "exit_code": None,
        "started_at": now_iso(),
        "completed_at": None,
        "execution_seconds": None,
    }
    exit_code = 1
    try:
        plan = load_plan(args.plan)
        if args.variant != plan["variant"]:
            raise RunError(f"workflow variant {args.variant!r} differs from plan {plan['variant']!r}")

        target_root = args.target_root.resolve()
        environment = validate_environment(target_root, plan)
        targets, selected, exit_code, elapsed, stats = execute(
            target_root,
            plan,
            args.variant,
            args.shard_index,
        )
        evidence.update(
            {
                "pair_id": plan["pair_id"],
                "order_position": plan["order_position"],
                "harness_base_sha": plan["harness_base_sha"],
                "harness_dispatch_sha": plan["harness_dispatch_sha"],
                "harness_logic_sha256": plan["harness_logic_sha256"],
                "target_sha": plan["target_sha"],
                "target_count": len(targets),
                "target_digest_sha256": target_digest(targets),
                "all_targets": targets,
                "selected_target_count": len(selected),
                "selected_targets": selected,
                "selected_target_digest_sha256": target_digest(selected),
                "environment": environment,
                "command_semantics": (
                    "production run-integration-shard.py <index> 4"
                    if args.variant == "A"
                    else "cargo test --workspace --all-features with every discovered --test selector"
                ),
                "status": "success" if exit_code == 0 else "test-failure",
                "exit_code": exit_code,
                "execution_seconds": round(elapsed, 3),
                "sccache_stats": stats,
            }
        )
    except (RunError, subprocess.CalledProcessError, OSError, ValueError) as exc:
        evidence["error"] = str(exc)
        print(f"::error::{exc}")
        exit_code = 1
    finally:
        evidence["completed_at"] = now_iso()
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote research evidence: {args.out}")

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
