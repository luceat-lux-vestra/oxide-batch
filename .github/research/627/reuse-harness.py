#!/usr/bin/env python3
"""Research-only A4-vs-C3 integration topology harness for oxide-batch #627."""
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
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "oxide-batch-topology/v1"
DISPATCH_SCHEMA = "oxide-batch-integration-topology-dispatch/v1"
RESEARCH = Path(".github/research/627")
WORKFLOW = Path(".github/workflows/research-627-integration-reuse.yml")
DISPATCH = Path(".github/research/627-dispatch.json")
IDENTITY_KEYS = (
    "harness_head_sha",
    "harness_logic_sha256",
    "target_sha",
    "target_count",
    "target_digest_sha256",
    "toolchain_sha256",
    "cargo_lock_sha256",
    "variant",
    "lane_count",
)


def fail(message: str) -> None:
    raise ValueError(message)


def run(command: list[str], cwd: Path | None = None, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, check=True, text=True, capture_output=capture)


def git(root: Path, *args: str) -> str:
    return run(["git", "-C", str(root), *args], capture=True).stdout.strip()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        fail(f"{path}: expected JSON object")
    return value


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def production_sharder(root: Path):
    path = root / ".github/scripts/run-integration-shard.py"
    spec = importlib.util.spec_from_file_location("oxide_batch_production_sharder_627", path)
    if not path.is_file() or spec is None or spec.loader is None:
        fail("production sharder unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if getattr(module, "SHARD_COUNT", None) != 4:
        fail("production SHARD_COUNT must remain 4 for A4 control")
    if not isinstance(getattr(module, "HISTORICAL_SECONDS", None), dict):
        fail("production historical weights unavailable")
    return module


def exact_cover(targets: list[str], partitions: list[list[str]], expected_count: int) -> None:
    if len(partitions) != expected_count or any(not lane for lane in partitions):
        fail("partition count mismatch or empty lane")
    flattened = [name for lane in partitions for name in lane]
    counts = Counter(flattened)
    if set(flattened) != set(targets) or len(flattened) != len(targets):
        fail("partition does not cover the target inventory")
    if any(count != 1 for count in counts.values()):
        fail("partition is not exact one-to-one cover")


def generic_partition(targets: list[str], weights: dict[str, float], count: int) -> tuple[list[list[str]], list[float]]:
    if count < 1:
        fail("lane count must be positive")
    if len(targets) < count:
        fail("lane count exceeds target count")
    lanes: list[list[str]] = [[] for _ in range(count)]
    loads = [0.0] * count
    ordered = sorted(targets, key=lambda name: (-float(weights.get(name, 0.10)), name))
    for name in ordered:
        lane = min(range(count), key=lambda index: (loads[index], index))
        lanes[lane].append(name)
        loads[lane] += float(weights.get(name, 0.10))
    lanes = [sorted(lane) for lane in lanes]
    exact_cover(targets, lanes, count)
    return lanes, [round(value, 3) for value in loads]


def inventory(root: Path, variant: str) -> tuple[list[str], list[list[str]], list[float]]:
    sharder = production_sharder(root)
    old = Path.cwd()
    try:
        os.chdir(root)
        targets = sharder.load_targets()
        production = sharder.partition(targets)
    finally:
        os.chdir(old)
    exact_cover(targets, production, 4)
    weights = {str(key): float(value) for key, value in sharder.HISTORICAL_SECONDS.items()}
    if variant == "A4":
        lanes = production
        loads = [round(sum(weights.get(name, 0.10) for name in lane), 3) for lane in lanes]
    elif variant == "C3":
        lanes, loads = generic_partition(targets, weights, 3)
    else:
        fail(f"unsupported variant: {variant}")
    exact_cover(targets, lanes, 4 if variant == "A4" else 3)
    return targets, lanes, loads


def harness_identity(root: Path) -> tuple[dict[str, str], str]:
    tracked = [
        str(RESEARCH / "reuse-harness.py"),
        str(RESEARCH / "setup-sccache/action.yml"),
        str(WORKFLOW),
    ]
    for name in tracked:
        if not (root / name).is_file():
            fail(f"tracked harness file missing: {name}")
    files = {name: sha256(root / name) for name in tracked}
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode("utf-8") + b"\0" + files[name].encode("ascii") + b"\n")
    return files, digest.hexdigest()


def read_dispatch(path: Path) -> dict:
    data = load(path)
    expected = {"schema", "harness_base_sha", "target_sha", "experiment_id", "variant"}
    if set(data) != expected:
        fail(f"dispatch keys mismatch: {sorted(data)}")
    if data.get("schema") != DISPATCH_SCHEMA:
        fail("unsupported dispatch schema")
    for key in ("harness_base_sha", "target_sha"):
        value = data.get(key)
        if not isinstance(value, str) or len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
            fail(f"{key} must be lowercase 40-hex")
    experiment = data.get("experiment_id")
    allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
    if not isinstance(experiment, str) or not (1 <= len(experiment) <= 64) or any(c not in allowed for c in experiment):
        fail("experiment_id is invalid")
    if data.get("variant") not in ("A4", "C3"):
        fail("variant must be A4 or C3")
    return data


def validate_dispatch_delta(root: Path, base: str, head: str) -> list[str]:
    run(["git", "-C", str(root), "merge-base", "--is-ancestor", base, head])
    changed = [line for line in git(root, "diff", "--name-only", base, head).splitlines() if line]
    if changed != [str(DISPATCH)]:
        fail(f"dispatch head may differ from harness base only by {DISPATCH}: {changed}")
    return changed


def plan(args) -> int:
    root = args.harness_root.resolve()
    target = args.target_root.resolve()
    dispatch = read_dispatch(args.dispatch.resolve())
    head = args.head_sha
    base = dispatch["harness_base_sha"]
    target_sha = dispatch["target_sha"]
    if len(head) != 40 or any(c not in "0123456789abcdef" for c in head):
        fail("head SHA must be lowercase 40-hex")
    validate_dispatch_delta(root, base, head)
    if git(target, "rev-parse", "HEAD") != target_sha:
        fail("target checkout mismatch")
    run(["git", "-C", str(root), "merge-base", "--is-ancestor", target_sha, "origin/main"])
    if git(target, "status", "--porcelain", "--untracked-files=no"):
        fail("target checkout dirty")

    targets, partitions, modeled_loads = inventory(target, dispatch["variant"])
    files, digest = harness_identity(root)
    lane_count = len(partitions)
    target_digest = hashlib.sha256(("\n".join(targets) + "\n").encode("utf-8")).hexdigest()
    matrix = {"include": [{"lane": index} for index in range(lane_count)]}
    out = {
        "schema": SCHEMA,
        "kind": "plan",
        "experiment_id": dispatch["experiment_id"],
        "variant": dispatch["variant"],
        "lane_count": lane_count,
        "harness_base_sha": base,
        "harness_head_sha": head,
        "harness_logic_files": files,
        "harness_logic_sha256": digest,
        "target_sha": target_sha,
        "target_count": len(targets),
        "target_digest_sha256": target_digest,
        "targets": targets,
        "partitions": partitions,
        "partition_sizes": [len(lane) for lane in partitions],
        "modeled_loads": modeled_loads,
        "toolchain_sha256": sha256(target / "rust-toolchain.toml"),
        "cargo_lock_sha256": sha256(target / "Cargo.lock"),
        "matrix": matrix,
    }
    args.out.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"target_sha={target_sha}\n")
            output.write(f"variant={dispatch['variant']}\n")
            output.write(f"lane_count={lane_count}\n")
            output.write(f"matrix={json.dumps(matrix, separators=(',', ':'))}\n")
            output.write(f"harness_base_sha={base}\n")
            output.write(f"harness_logic_digest={digest}\n")
            output.write(f"target_count={len(targets)}\n")
            output.write(f"target_digest={target_digest}\n")
            output.write(f"experiment_id={dispatch['experiment_id']}\n")
    return 0


def identity(plan_data: dict) -> dict:
    return {key: plan_data[key] for key in IDENTITY_KEYS}


def same_identity(plan_data: dict, other: dict) -> None:
    for key in IDENTITY_KEYS:
        if other.get(key) != plan_data.get(key):
            fail(f"{key} mismatch")


def cargo_command(selected: list[str]) -> list[str]:
    command = ["cargo", "test", "--workspace", "--all-features"]
    for name in selected:
        command.extend(["--test", name])
    return command


def lane(args) -> int:
    plan_data = load(args.plan)
    root = args.target_root.resolve()
    if plan_data.get("schema") != SCHEMA or plan_data.get("kind") != "plan":
        fail("invalid plan")
    if git(root, "rev-parse", "HEAD") != plan_data["target_sha"]:
        fail("lane target checkout mismatch")
    if os.getenv("RUNNER_ARCH") != "ARM64" or platform.machine() != "aarch64":
        fail("ARM64 required")
    if git(root, "status", "--porcelain", "--untracked-files=no"):
        fail("lane source dirty before execution")
    targets, partitions, modeled_loads = inventory(root, plan_data["variant"])
    if targets != plan_data["targets"] or partitions != plan_data["partitions"] or modeled_loads != plan_data["modeled_loads"]:
        fail("lane inventory/partition drift")
    if args.lane not in range(plan_data["lane_count"]):
        fail("lane index out of range")
    selected = partitions[args.lane]
    command = (
        ["python3", ".github/scripts/run-integration-shard.py", str(args.lane), "4"]
        if plan_data["variant"] == "A4"
        else cargo_command(selected)
    )
    run(["sccache", "--zero-stats"])
    started_at = now()
    started = time.monotonic()
    proc = subprocess.run(command, cwd=root)
    elapsed = round(time.monotonic() - started, 3)
    stats = json.loads(run(["sccache", "--show-stats", "--stats-format=json"], capture=True).stdout)
    source_clean = not bool(git(root, "status", "--porcelain", "--untracked-files=no"))
    evidence = {
        **identity(plan_data),
        "schema": SCHEMA,
        "kind": "lane",
        "status": "success" if proc.returncode == 0 and source_clean else "failed",
        "exit_code": proc.returncode,
        "lane_index": args.lane,
        "selected_targets": selected,
        "selected_target_count": len(selected),
        "modeled_load": modeled_loads[args.lane],
        "command": command,
        "execution_seconds": elapsed,
        "started_at": started_at,
        "completed_at": now(),
        "source_clean_after_execution": source_clean,
        "runner_arch": os.getenv("RUNNER_ARCH"),
        "uname_machine": platform.machine(),
        "rustc": run(["rustc", "--version"], capture=True).stdout.strip(),
        "cargo": run(["cargo", "--version"], capture=True).stdout.strip(),
        "sccache": run(["sccache", "--version"], capture=True).stdout.strip(),
        "sccache_stats": stats,
        "cache": {
            key: os.getenv(key)
            for key in ("RESEARCH_CACHE_KEY", "RESEARCH_CACHE_MATCHED_KEY", "RESEARCH_CACHE_HIT")
        },
    }
    args.out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if evidence["status"] == "success" else 1


def validate_summary(plan_data: dict, items: list[dict]) -> dict:
    lane_count = plan_data["lane_count"]
    if len(items) != lane_count:
        fail(f"expected {lane_count} lane evidence files")
    by_lane: dict[int, dict] = {}
    for item in items:
        same_identity(plan_data, item)
        index = item.get("lane_index")
        if (
            item.get("kind") != "lane"
            or item.get("status") != "success"
            or item.get("exit_code") != 0
            or index not in range(lane_count)
            or index in by_lane
            or item.get("selected_targets") != plan_data["partitions"][index]
            or item.get("source_clean_after_execution") is not True
            or not isinstance(item.get("sccache_stats"), dict)
        ):
            fail("invalid lane evidence")
        by_lane[index] = item
    flattened = [name for index in range(lane_count) for name in by_lane[index]["selected_targets"]]
    counts = Counter(flattened)
    if set(flattened) != set(plan_data["targets"]) or len(flattened) != len(plan_data["targets"]):
        fail("lane evidence does not cover exact target inventory")
    if any(count != 1 for count in counts.values()):
        fail("lane evidence contains duplicate target execution")
    execution = [float(by_lane[index]["execution_seconds"]) for index in range(lane_count)]
    return {
        "execution_seconds": execution,
        "execution_critical_seconds": max(execution),
        "execution_sum_seconds": round(sum(execution), 3),
        "partition_sizes": plan_data["partition_sizes"],
        "modeled_loads": plan_data["modeled_loads"],
        "lane_cache": [
            {
                "lane_index": index,
                "sccache_stats": by_lane[index]["sccache_stats"],
                "cache": by_lane[index]["cache"],
            }
            for index in range(lane_count)
        ],
    }


def summarize(args) -> int:
    plan_data = load(args.plan)
    items = [load(path) for path in sorted(args.evidence_dir.glob("evidence-topology-*.json"))]
    out = {
        "schema": SCHEMA,
        "kind": "summary",
        "valid": False,
        "variant": plan_data.get("variant"),
        "target_sha": plan_data.get("target_sha"),
        "target_count": plan_data.get("target_count"),
        "target_digest_sha256": plan_data.get("target_digest_sha256"),
        "harness_logic_sha256": plan_data.get("harness_logic_sha256"),
    }
    try:
        out.update(validate_summary(plan_data, items))
        out["valid"] = True
    except Exception as exc:
        out["error"] = str(exc)
    args.out.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if out["valid"] else 1


class Tests(unittest.TestCase):
    def test_generic_partition_exact_cover(self) -> None:
        targets = ["ui", "a", "b", "c", "d", "e"]
        weights = {"ui": 10.0, "a": 3.0, "b": 2.0}
        lanes, loads = generic_partition(targets, weights, 3)
        exact_cover(targets, lanes, 3)
        self.assertEqual(lanes[0], ["ui"])
        self.assertEqual(len(loads), 3)

    def test_command_surface(self) -> None:
        self.assertEqual(
            cargo_command(["a", "b"]),
            ["cargo", "test", "--workspace", "--all-features", "--test", "a", "--test", "b"],
        )

    def test_dispatch_contract(self) -> None:
        good = {
            "schema": DISPATCH_SCHEMA,
            "harness_base_sha": "a" * 40,
            "target_sha": "b" * 40,
            "experiment_id": "pair01-A4",
            "variant": "A4",
        }
        self.assertEqual(set(good), {"schema", "harness_base_sha", "target_sha", "experiment_id", "variant"})
        self.assertIn(good["variant"], ("A4", "C3"))

    def test_workflow_is_research_dispatch_only(self) -> None:
        workflow = (Path(__file__).resolve().parents[2] / "workflows" / WORKFLOW.name).read_text(encoding="utf-8")
        self.assertIn("branches: [research/627-integration-reuse]", workflow)
        self.assertIn("paths: [.github/research/627-dispatch.json]", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertNotIn("workflow_dispatch:", workflow)
        self.assertIn("fromJSON(needs.plan.outputs.matrix)", workflow)
        self.assertIn("setup-sccache", workflow)
        self.assertNotIn("actions/cache/save", workflow)
        self.assertNotIn("seed-workspace", workflow)


def self_test(_) -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="cmd", required=True)

    command = commands.add_parser("plan")
    command.add_argument("--harness-root", type=Path, required=True)
    command.add_argument("--target-root", type=Path, required=True)
    command.add_argument("--dispatch", type=Path, required=True)
    command.add_argument("--head-sha", required=True)
    command.add_argument("--out", type=Path, required=True)
    command.add_argument("--github-output", type=Path)
    command.set_defaults(handler=plan)

    command = commands.add_parser("run-lane")
    command.add_argument("--plan", type=Path, required=True)
    command.add_argument("--target-root", type=Path, required=True)
    command.add_argument("--lane", type=int, required=True)
    command.add_argument("--out", type=Path, required=True)
    command.set_defaults(handler=lane)

    command = commands.add_parser("summarize")
    command.add_argument("--plan", type=Path, required=True)
    command.add_argument("--evidence-dir", type=Path, required=True)
    command.add_argument("--out", type=Path, required=True)
    command.set_defaults(handler=summarize)

    command = commands.add_parser("self-test")
    command.set_defaults(handler=self_test)
    return root


def main() -> None:
    try:
        args = parser().parse_args()
        code = args.handler(args)
    except Exception as exc:
        print(f"::error::{exc}")
        code = 1
    raise SystemExit(code or 0)


if __name__ == "__main__":
    main()
