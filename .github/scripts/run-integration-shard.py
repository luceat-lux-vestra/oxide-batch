#!/usr/bin/env python3
"""Run one deterministic shard of the workspace integration-test targets."""

from __future__ import annotations

import json
import subprocess
import sys
from collections import Counter
from typing import Iterable

SHARD_COUNT = 4

# Relative weights from the first parallel-quality run on PR #332. They are
# scheduling hints only; every metadata-discovered integration target remains
# in the exact partition even when it has no recorded weight.
HISTORICAL_SECONDS = {
    "ui": 122.50,
    "gate_h_throughput": 20.56,
    "item_components_json_allocation": 13.45,
    "item_components_flat_file_allocation": 9.54,
    "m4_exit_measurements": 5.87,
    "merge_gate_policy": 3.97,
    "m5_resource_bounds_campaign": 1.36,
    "gate_h_allocation": 1.19,
    "postgres_performance": 0.98,
    "item_components_allocation": 0.65,
    "chunk_allocation": 0.63,
}


def load_targets() -> list[str]:
    raw = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    metadata = json.loads(raw)
    owners: dict[str, list[str]] = {}
    for package in metadata["packages"]:
        for target in package["targets"]:
            if "test" not in target.get("kind", []):
                continue
            owners.setdefault(target["name"], []).append(package["name"])

    duplicates = {name: packages for name, packages in owners.items() if len(packages) != 1}
    if duplicates:
        rendered = ", ".join(
            f"{name}=>{sorted(packages)}" for name, packages in sorted(duplicates.items())
        )
        raise SystemExit(
            "integration target names must be workspace-unique before name-based sharding: "
            + rendered
        )
    targets = sorted(owners)
    if not targets:
        raise SystemExit("cargo metadata reported no workspace integration-test targets")
    return targets


def partition(targets: Iterable[str]) -> list[list[str]]:
    shards: list[list[str]] = [[] for _ in range(SHARD_COUNT)]
    loads = [0.0] * SHARD_COUNT
    ordered = sorted(
        targets,
        key=lambda name: (-HISTORICAL_SECONDS.get(name, 0.10), name),
    )
    for name in ordered:
        shard = min(range(SHARD_COUNT), key=lambda index: (loads[index], index))
        shards[shard].append(name)
        loads[shard] += HISTORICAL_SECONDS.get(name, 0.10)

    flattened = [name for shard in shards for name in shard]
    counts = Counter(flattened)
    target_set = set(targets)
    if set(flattened) != target_set or any(count != 1 for count in counts.values()):
        raise SystemExit("integration shard partition is not an exact one-to-one cover")
    if any(not shard for shard in shards):
        raise SystemExit("integration shard partition produced an empty shard")
    return [sorted(shard) for shard in shards]


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: run-integration-shard.py <shard-index> <shard-count>")
    shard_index = int(sys.argv[1])
    requested_count = int(sys.argv[2])
    if requested_count != SHARD_COUNT or shard_index not in range(SHARD_COUNT):
        raise SystemExit(
            f"expected shard-index 0..{SHARD_COUNT - 1} and shard-count {SHARD_COUNT}"
        )

    targets = load_targets()
    shards = partition(targets)
    selected = shards[shard_index]
    print(
        f"integration shard {shard_index}/{SHARD_COUNT}: "
        f"{len(selected)} of {len(targets)} targets"
    )
    for name in selected:
        print(f"  {name}")

    command = ["cargo", "test", "--workspace", "--all-features"]
    for name in selected:
        command.extend(["--test", name])
    subprocess.run(command, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
