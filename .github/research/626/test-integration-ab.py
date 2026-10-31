#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_module(filename: str, name: str):
    path = ROOT / filename
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


planner = load_module("plan-integration-ab.py", "integration_ab_planner")
summary = load_module("summarize-integration-ab.py", "integration_ab_summary")


def plan(variant: str) -> dict:
    targets = ["alpha", "beta", "gamma", "delta"]
    partitions = [["alpha"], ["beta"], ["gamma"], ["delta"]]
    return {
        "schema": "oxide-batch-integration-ab-plan/v1",
        "variant": variant,
        "pair_id": "pair-01-AB",
        "order_position": 1 if variant == "A" else 2,
        "harness_base_sha": "a" * 40,
        "harness_dispatch_sha": "b" * 40,
        "harness_logic_sha256": "c" * 64,
        "target_sha": "d" * 40,
        "target_count": len(targets),
        "target_digest_sha256": "e" * 64,
        "targets": targets,
        "partitions": partitions,
        "partition_sizes": [1, 1, 1, 1],
    }


def evidence(p: dict, *, shard_index: int | None, selected: list[str]) -> dict:
    return {
        "schema": "oxide-batch-integration-ab-evidence/v1",
        "variant": p["variant"],
        "shard_index": shard_index,
        "status": "success",
        "exit_code": 0,
        "pair_id": p["pair_id"],
        "order_position": p["order_position"],
        "harness_base_sha": p["harness_base_sha"],
        "harness_dispatch_sha": p["harness_dispatch_sha"],
        "harness_logic_sha256": p["harness_logic_sha256"],
        "target_sha": p["target_sha"],
        "target_digest_sha256": p["target_digest_sha256"],
        "all_targets": p["targets"],
        "selected_targets": selected,
        "execution_seconds": 1.0,
        "environment": {
            "cache_key": "requested",
            "cache_matched_key": "matched",
            "cache_hit": "true",
        },
        "sccache_stats": {"compile_requests": 1},
    }


class DispatchTests(unittest.TestCase):
    def read(self, value: dict):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "dispatch.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            return planner.read_dispatch(path)

    def valid(self, *, variant: str = "A", pair_id: str = "pair-01-AB", order: int = 1) -> dict:
        return {
            "schema": "oxide-batch-integration-ab/v1",
            "harness_base_sha": "a" * 40,
            "target_sha": "b" * 40,
            "variant": variant,
            "pair_id": pair_id,
            "order_position": order,
        }

    def test_ab_and_ba_order_contract(self) -> None:
        self.assertEqual(self.read(self.valid())["variant"], "A")
        self.assertEqual(
            self.read(self.valid(variant="B", pair_id="pair-02-BA", order=1))["variant"],
            "B",
        )
        with self.assertRaises(planner.PlanError):
            self.read(self.valid(variant="B", pair_id="pair-01-AB", order=1))
        with self.assertRaises(planner.PlanError):
            self.read(self.valid(variant="A", pair_id="pair-02-BA", order=1))
        with self.assertRaises(planner.PlanError):
            self.read(self.valid(pair_id="pair-03", order=1))

    def test_rejects_extra_keys_and_non_sha_refs(self) -> None:
        bad = self.valid()
        bad["extra"] = True
        with self.assertRaises(planner.PlanError):
            self.read(bad)
        bad = self.valid()
        bad["target_sha"] = "main"
        with self.assertRaises(planner.PlanError):
            self.read(bad)


class EvidenceTests(unittest.TestCase):
    def test_a_requires_exact_four_way_cover(self) -> None:
        p = plan("A")
        items = [
            evidence(p, shard_index=index, selected=p["partitions"][index])
            for index in range(4)
        ]
        result = summary.validate_evidence(p, items)
        self.assertEqual(result["execution_critical_seconds"], 1.0)
        self.assertEqual(result["execution_sum_seconds"], 4.0)

    def test_a_rejects_duplicate_or_missing_target(self) -> None:
        p = plan("A")
        items = [
            evidence(p, shard_index=index, selected=p["partitions"][index])
            for index in range(4)
        ]
        items[3]["selected_targets"] = ["alpha"]
        with self.assertRaises(summary.SummaryError):
            summary.validate_evidence(p, items)

    def test_b_requires_full_planned_inventory(self) -> None:
        p = plan("B")
        item = evidence(p, shard_index=None, selected=p["targets"])
        result = summary.validate_evidence(p, [item])
        self.assertEqual(result["execution_sum_seconds"], 1.0)

        item["selected_targets"] = p["targets"][:-1]
        with self.assertRaises(summary.SummaryError):
            summary.validate_evidence(p, [item])

    def test_failed_execution_and_missing_sccache_counters_fail_closed(self) -> None:
        p = plan("B")
        item = evidence(p, shard_index=None, selected=p["targets"])
        item["status"] = "test-failure"
        item["exit_code"] = 1
        with self.assertRaises(summary.SummaryError):
            summary.validate_evidence(p, [item])

        item = evidence(p, shard_index=None, selected=p["targets"])
        item["sccache_stats"] = {}
        with self.assertRaises(summary.SummaryError):
            summary.validate_evidence(p, [item])


class HarnessIdentityTests(unittest.TestCase):
    def test_generated_pycache_does_not_change_harness_identity(self) -> None:
        repo_root = ROOT.parents[2]
        before_files, before_digest = planner.harness_logic(repo_root)
        generated = ROOT / "__pycache__" / "identity-regression.pyc"
        existed = generated.exists()
        original = generated.read_bytes() if existed else None
        try:
            generated.parent.mkdir(parents=True, exist_ok=True)
            generated.write_bytes(b"volatile generated bytecode\n")
            after_files, after_digest = planner.harness_logic(repo_root)
        finally:
            if existed:
                generated.write_bytes(original)
            else:
                generated.unlink(missing_ok=True)

        generated_rel = generated.relative_to(repo_root).as_posix()
        self.assertEqual(before_files, after_files)
        self.assertEqual(before_digest, after_digest)
        self.assertNotIn(generated_rel, after_files)
        self.assertFalse(any("__pycache__" in path for path in after_files))
        self.assertIn(".github/research/626/test-integration-ab.py", after_files)
        self.assertIn(".github/workflows/research-626-integration-shard-ab.yml", after_files)


class WorkflowTopologyTests(unittest.TestCase):
    def test_research_trigger_is_branch_and_dispatch_path_only(self) -> None:
        workflow = (ROOT.parents[1] / "workflows" / "research-626-integration-shard-ab.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("branches: [research/626-integration-shard-ab]", workflow)
        self.assertIn("paths: [.github/research/626-dispatch.json]", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertNotIn("workflow_dispatch:", workflow)
        self.assertIn("if: needs.plan.outputs.variant == 'A'", workflow)
        self.assertIn("if: needs.plan.outputs.variant == 'B'", workflow)
        self.assertIn("matrix:", workflow)
        self.assertIn("shard: [0, 1, 2, 3]", workflow)
        self.assertIn("summarize-integration-ab.py", workflow)


if __name__ == "__main__":
    unittest.main()
