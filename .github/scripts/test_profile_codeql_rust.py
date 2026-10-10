#!/usr/bin/env python3
"""Offline adversarial tests for the opt-in Rust CodeQL profiler."""
import importlib.util
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE = Path(__file__).with_name("profile_codeql_rust.py")
spec = importlib.util.spec_from_file_location("profile_codeql_rust", MODULE)
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="codeql-offline-test-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.db = self.base / "rust-db"
        self.db.mkdir()
        (self.db / "codeql-database.yml").write_text("primaryLanguage: rust\n")
        self.cli = self.base / "codeql"
        self.log = self.base / "mock-input.log"
        lines = []
        for i, query in enumerate(sorted(profile.EXPECTED_QUERIES), 1):
            lines.append(f"[{i}/37] Loaded /opt/codeql/qlpacks/{profile.EXPECTED_PACK}/{query}.")
        for i in range(1, 38):
            lines.append(f"[{i}/37 eval 1s] Evaluation done; writing results to foo.bqrs.")
        self.log.write_text("\n".join(lines))
        self.cli.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
if args == ["version"]:
    print("CodeQL command-line toolchain release " + os.environ.get("MOCK_VERSION", "2.27.2"))
elif args[:2] == ["database", "run-queries"]:
    pathlib.Path(os.environ["MOCK_ARGS"]).write_text(json.dumps(args))
    path = next(arg.split("=", 1)[1] for arg in args if arg.startswith("--evaluator-log="))
    pathlib.Path(path).write_text('{"event": "example"}')
    print(pathlib.Path(os.environ["MOCK_LOG"]).read_text(), end="")
    sys.exit(int(os.environ.get("MOCK_FAIL", "0")))
elif args[:2] == ["generate", "log-summary"]:
    pathlib.Path(args[-1]).write_text(os.environ.get("MOCK_SUMMARY", json.dumps({
        "mostExpensivePerQuery": [
            {"query": "queries/security/CWE-078/CommandInjection.qlx",
             "totalTimeMs": 1123, "rawSource": "PRIVATE_USER_CODE"},
            {"query": "unknown/internal/query", "cpuTime": 22}],
        "mostExpensivePerStage": [
            {"durationMs": 420, "stageName": "PRIVATE_STORAGE_PATH"}],
        "stats": {"totalTimeMs": 1200, "secret": "PRIVATE_INTERNAL_HOST"}
    })))
else:
    sys.exit(18)
''')
        self.cli.chmod(self.cli.stat().st_mode | stat.S_IXUSR)
        self.env = {"MOCK_LOG": str(self.log), "MOCK_ARGS": str(self.base/"args.json")}

    def invoke(self, output_name="out", **env):
        argv = ["--codeql", str(self.cli), "--database", str(self.db),
                "--output-dir", str(self.base/output_name)]
        with patch.dict(os.environ, {**self.env, **env}):
            return profile.main(argv)

    def test_success_is_profile_only_with_exact_manifest(self):
        self.assertEqual(self.invoke(), 0)
        report = json.loads((self.base/"out/metadata.json").read_text())
        self.assertEqual(report["query_count"], 37)
        self.assertIn("NOT_A_SECURITY_SCAN", report["verdict"])
        args = json.loads((self.base/"args.json").read_text())
        self.assertIn("--ram=14535", args)
        self.assertIn("--threads=4", args)
        self.assertNotIn("--no-rerun", args)
        self.assertEqual((self.base/"out").stat().st_mode & 0o777, 0o700)

    def test_sanitized_hotspots_contains_only_allowlisted_numeric_metrics(self):
        self.assertEqual(self.invoke(), 0)
        path = self.base / "out/sanitized-hotspots.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        safe = json.loads(path.read_text())
        self.assertEqual(safe["query_ranks"]["items"][0]["numeric_metrics"],
                         {"totalTimeMs": 1123})
        self.assertEqual(safe["query_ranks"]["items"][0]["query_id_from_frozen_manifest"],
                         "queries/security/CWE-078/CommandInjection.qlx")
        self.assertNotIn("query_id_from_frozen_manifest", safe["query_ranks"]["items"][1])
        self.assertEqual(safe["stage_ranks"]["items"][0]["numeric_metrics"],
                         {"durationMs": 420})
        serialized = json.dumps(safe)
        for secret in ("PRIVATE_USER_CODE", "PRIVATE_STORAGE_PATH", "PRIVATE_INTERNAL_HOST", "unknown/internal"):
            self.assertNotIn(secret, serialized)
        meta = json.loads((self.base / "out/metadata.json").read_text())
        self.assertEqual(len(meta["sanitized_report_sha256"]), 64)
        self.assertEqual(meta["sanitized_report_schema"], 1)

    def test_unrecognized_overall_shape_fails_closed_without_metadata(self):
        self.assertEqual(self.invoke(MOCK_SUMMARY=json.dumps({"overall": {"time": 9}})), 1)
        self.assertTrue((self.base / "out/INCOMPLETE_DO_NOT_USE").exists())
        self.assertFalse((self.base / "out/metadata.json").exists())

    def test_no_unrecognized_strings_or_false_numbers_escape(self):
        data = {"mostExpensivePerQuery": [
            {"query": "SECRET_CUSTOM_QUERY", "totalTimeMs": True,
             "durationMs": -1, "cpuTime": 42, "message": "SECRET_SQL"}],
                "mostExpensivePerStage": [{"stageName": "SECRET_PATH", "time": 10}],
                "stats": {"totalTimeMs": 50, "sourceSnippet": "SECRET_SOURCE"}}
        self.assertEqual(self.invoke(MOCK_SUMMARY=json.dumps(data)), 0)
        payload = (self.base / "out/sanitized-hotspots.json").read_text()
        self.assertNotIn("SECRET", payload)
        self.assertNotIn("-1", payload)
        self.assertIn('"cpuTime": 42', payload)

    def test_no_numeric_performance_fields_fails_closed(self):
        data = {"mostExpensivePerQuery": [{"query": "SENSITIVE", "durationMs": "x"}],
                "mostExpensivePerStage": [], "stats": {"secret": 1234}}
        self.assertEqual(self.invoke(MOCK_SUMMARY=json.dumps(data)), 1)
        self.assertTrue((self.base / "out/INCOMPLETE_DO_NOT_USE").exists())

    def test_summary_is_bounded_and_does_not_mislabel_object_order(self):
        data = {"mostExpensivePerQuery": [{"timeMs": i, "trace": "TOP_SECRET"}
                 for i in range(20)],
                "mostExpensivePerStage": {"SECRET_KEY": {"time": 2}},
                "stats": {"duration": 9}}
        self.assertEqual(self.invoke(MOCK_SUMMARY=json.dumps(data)), 0)
        result = json.loads((self.base / "out/sanitized-hotspots.json").read_text())
        self.assertEqual(len(result["query_ranks"]["items"]), 5)
        self.assertEqual(result["query_ranks"]["source_item_count"], 20)
        self.assertEqual(result["stage_ranks"]["order"], "object_iteration_order_unranked")
        self.assertNotIn("SECRET_KEY", json.dumps(result))

    def test_stats_only_metrics_does_not_falsely_validate_hotspots(self):
        data = {"mostExpensivePerQuery": [{"query": "SENSITIVE", "durationMs": "unknown"}],
                "mostExpensivePerStage": [{"time": 10}],
                "stats": {"totalTimeMs": 100}}
        self.assertEqual(self.invoke(MOCK_SUMMARY=json.dumps(data)), 1)
        self.assertTrue((self.base / "out/INCOMPLETE_DO_NOT_USE").is_file())
        self.assertFalse((self.base / "out/sanitized-hotspots.json").exists())

    def test_missing_one_query_fails_closed(self):
        self.log.write_text("\n".join(self.log.read_text().splitlines()[1:]))
        self.assertEqual(self.invoke(), 1)
        self.assertTrue((self.base/"out/INCOMPLETE_DO_NOT_USE").is_file())

    def test_changed_pack_fails_closed(self):
        self.log.write_text(self.log.read_text().replace("rust-queries/0.1.44", "rust-queries/0.1.43"))
        self.assertEqual(self.invoke(), 1)

    def test_fail_command_cannot_produce_green_report(self):
        self.assertEqual(self.invoke(MOCK_FAIL="12"), 1)
        self.assertFalse((self.base/"out/metadata.json").exists())

    def test_mismatched_version_fails_before_output_creation(self):
        self.assertEqual(self.invoke(MOCK_VERSION="2.27.1"), 1)
        self.assertFalse((self.base/"out").exists())

    def test_input_safety(self):
        self.assertEqual(self.invoke(), 0)
        self.assertEqual(self.invoke(), 1)  # never overwrite data
        (self.db / "codeql-database.yml").write_text("primaryLanguage: actions\n")
        self.assertEqual(self.invoke("other"), 1)

    def test_manifest_duplicate_does_not_pass(self):
        p = sorted(profile.EXPECTED_QUERIES)
        self.log.write_text(self.log.read_text().replace(p[1], p[0]))
        self.assertEqual(self.invoke(), 1)

    def test_baseline_manifest_frozen(self):
        self.assertEqual(len(profile.EXPECTED_QUERIES), 37)
        self.assertEqual(sum("/security/" in s for s in profile.EXPECTED_QUERIES), 17)


if __name__ == "__main__":
    unittest.main()
