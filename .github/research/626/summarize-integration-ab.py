#!/usr/bin/env python3
"""Validate #626 A/B evidence and add GitHub job/step timing diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path


class SummaryError(ValueError):
    pass


def parse_time(value: str | None) -> datetime:
    if not value:
        raise SummaryError("missing GitHub timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def seconds(start: str | None, end: str | None) -> float:
    return round((parse_time(end) - parse_time(start)).total_seconds(), 3)


def load_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SummaryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SummaryError(f"{path} must contain a JSON object")
    return value


def fetch_github_json(path: str) -> dict:
    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repository:
        raise SummaryError("GITHUB_TOKEN and GITHUB_REPOSITORY are required")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "oxide-batch-626-research",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except Exception as exc:
        raise SummaryError(f"GitHub API read failed for {path}: {exc}") from exc


def validate_evidence(plan: dict, evidence: list[dict]) -> dict:
    variant = plan["variant"]
    expected_count = 4 if variant == "A" else 1
    if len(evidence) != expected_count:
        raise SummaryError(f"expected {expected_count} evidence files for variant {variant}, observed {len(evidence)}")

    for item in evidence:
        if item.get("schema") != "oxide-batch-integration-ab-evidence/v1":
            raise SummaryError("unexpected evidence schema")
        for key in (
            "pair_id",
            "order_position",
            "harness_base_sha",
            "harness_dispatch_sha",
            "harness_logic_sha256",
            "target_sha",
            "target_digest_sha256",
        ):
            expected = plan[
                {
                    "target_digest_sha256": "target_digest_sha256",
                }.get(key, key)
            ]
            if item.get(key) != expected:
                raise SummaryError(f"evidence {key} mismatch: expected={expected!r} observed={item.get(key)!r}")
        if item.get("variant") != variant:
            raise SummaryError("evidence variant differs from plan")
        if item.get("status") != "success" or item.get("exit_code") != 0:
            raise SummaryError(
                f"variant {variant} execution failed: shard={item.get('shard_index')} "
                f"status={item.get('status')!r} exit={item.get('exit_code')!r}"
            )
        if item.get("all_targets") != plan["targets"]:
            raise SummaryError("evidence all-target inventory differs from plan")
        if not isinstance(item.get("sccache_stats"), dict) or not item["sccache_stats"]:
            raise SummaryError("actual sccache JSON counters are missing")

    if variant == "A":
        by_shard: dict[int, dict] = {}
        for item in evidence:
            index = item.get("shard_index")
            if not isinstance(index, int) or index not in range(4) or index in by_shard:
                raise SummaryError(f"invalid or duplicate A shard index: {index!r}")
            if item.get("selected_targets") != plan["partitions"][index]:
                raise SummaryError(f"A shard {index} selection differs from production partition")
            by_shard[index] = item
        if set(by_shard) != {0, 1, 2, 3}:
            raise SummaryError(f"A shard set mismatch: {sorted(by_shard)}")
        flattened = [
            target
            for index in range(4)
            for target in by_shard[index]["selected_targets"]
        ]
        counts = Counter(flattened)
        if set(flattened) != set(plan["targets"]) or any(count != 1 for count in counts.values()):
            raise SummaryError("A evidence does not form an exact one-to-one target cover")
    else:
        item = evidence[0]
        if item.get("shard_index") is not None:
            raise SummaryError("B evidence unexpectedly has a shard index")
        if item.get("selected_targets") != plan["targets"]:
            raise SummaryError("B did not execute the complete planned target inventory")

    execution_seconds = [float(item["execution_seconds"]) for item in evidence]
    return {
        "execution_seconds": execution_seconds,
        "execution_critical_seconds": max(execution_seconds),
        "execution_sum_seconds": round(sum(execution_seconds), 3),
    }


def github_timing(variant: str) -> dict:
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id or not run_id.isdigit():
        raise SummaryError("GITHUB_RUN_ID is missing or invalid")

    run = fetch_github_json(f"/actions/runs/{run_id}")
    jobs_payload = fetch_github_json(f"/actions/runs/{run_id}/jobs?per_page=100")
    jobs = jobs_payload.get("jobs")
    if not isinstance(jobs, list):
        raise SummaryError("GitHub jobs payload is malformed")
    total = jobs_payload.get("total_count")
    if isinstance(total, int) and total != len(jobs):
        raise SummaryError(f"GitHub jobs response is incomplete: total={total} returned={len(jobs)}")

    expected_names = (
        {f"A shard {index}" for index in range(4)}
        if variant == "A"
        else {"B unsharded"}
    )
    selected = [job for job in jobs if job.get("name") in expected_names]
    if {job.get("name") for job in selected} != expected_names:
        raise SummaryError(
            f"missing/duplicate selected GitHub jobs: expected={sorted(expected_names)} "
            f"observed={sorted(job.get('name') for job in selected)}"
        )
    if len(selected) != len(expected_names):
        raise SummaryError("selected GitHub job names are not unique")

    run_created = run.get("created_at")
    job_records = []
    for job in selected:
        if job.get("status") != "completed" or job.get("conclusion") != "success":
            raise SummaryError(
                f"selected GitHub job did not succeed: {job.get('name')} "
                f"status={job.get('status')} conclusion={job.get('conclusion')}"
            )
        steps = []
        for step in job.get("steps") or []:
            if step.get("started_at") and step.get("completed_at"):
                steps.append(
                    {
                        "name": step.get("name"),
                        "conclusion": step.get("conclusion"),
                        "elapsed_seconds": seconds(step.get("started_at"), step.get("completed_at")),
                    }
                )
        job_records.append(
            {
                "name": job.get("name"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "queue_offset_seconds": seconds(run_created, job.get("started_at")),
                "job_elapsed_seconds": seconds(job.get("started_at"), job.get("completed_at")),
                "steps": steps,
            }
        )

    elapsed = [record["job_elapsed_seconds"] for record in job_records]
    return {
        "workflow_run_id": int(run_id),
        "workflow_created_at": run_created,
        "jobs": sorted(job_records, key=lambda item: item["name"]),
        "job_critical_seconds": max(elapsed),
        "job_sum_seconds": round(sum(elapsed), 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    output: dict = {
        "schema": "oxide-batch-integration-ab-summary/v1",
        "valid": False,
    }
    exit_code = 1
    try:
        plan = load_json(args.plan)
        if plan.get("schema") != "oxide-batch-integration-ab-plan/v1":
            raise SummaryError("unexpected plan schema")
        evidence_paths = sorted(args.evidence_dir.rglob("evidence-*.json"))
        evidence = [load_json(path) for path in evidence_paths]
        execution = validate_evidence(plan, evidence)
        timing = github_timing(plan["variant"])

        output.update(
            {
                "valid": True,
                "variant": plan["variant"],
                "pair_id": plan["pair_id"],
                "order_position": plan["order_position"],
                "harness_base_sha": plan["harness_base_sha"],
                "harness_dispatch_sha": plan["harness_dispatch_sha"],
                "harness_logic_sha256": plan["harness_logic_sha256"],
                "target_sha": plan["target_sha"],
                "target_count": plan["target_count"],
                "target_digest_sha256": plan["target_digest_sha256"],
                "partition_sizes": plan["partition_sizes"],
                "execution": execution,
                "github_timing": timing,
                "cache_observations": [
                    {
                        "shard_index": item.get("shard_index"),
                        "cache_key": (item.get("environment") or {}).get("cache_key"),
                        "cache_matched_key": (item.get("environment") or {}).get("cache_matched_key"),
                        "cache_hit": (item.get("environment") or {}).get("cache_hit"),
                        "sccache_stats": item.get("sccache_stats"),
                    }
                    for item in sorted(evidence, key=lambda item: (-1 if item.get("shard_index") is None else item["shard_index"]))
                ],
            }
        )
        exit_code = 0
    except (SummaryError, OSError, ValueError) as exc:
        output["error"] = str(exc)
        print(f"::error::{exc}")
    finally:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote research summary: {args.out}")

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
