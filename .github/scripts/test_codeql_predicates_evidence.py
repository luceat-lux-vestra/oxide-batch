#!/usr/bin/env python3
"""Synthetic adversarial contract tests, not live CLI schema certification."""
from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("codeql_predicates_evidence.py")
spec = importlib.util.spec_from_file_location("codeql_predicates_evidence", SCRIPT)
assert spec and spec.loader
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


def event(strategy="COMPUTE_SIMPLE", **extra):
    value = {"evaluationStrategy": strategy,
             "predicateName": "PRIVATE::secret/source/path.rs", "raHash": "PRIVATEHASH"}
    if strategy in ("COMPUTE_SIMPLE", "COMPUTE_RECURSIVE"):
        value["millis"] = 120.25
    if strategy in ("COMPUTE_RECURSIVE", "IN_LAYER", "NAMED_LOCAL"):
        value["predicateIterationMillis"] = [3, -1, 8]
    value.update(extra)
    return value


def payload(*entries, pretty=False):
    return ("\n\n" if pretty else "\n").join(
        json.dumps(e, indent=2 if pretty else None) for e in entries) + "\n"


class Tests(unittest.TestCase):
    def valid(self, value):
        return p.analyze(io.StringIO(value))

    def fails(self, value):
        with self.assertRaises(p.IncompleteEvidence):
            self.valid(value)

    def test_one_compute_simple(self):
        d = self.valid(payload(event()))
        self.assertEqual(d["compute_simple_largest_five_millis_anonymous"], [120.25])
        self.assertEqual(d["records_checked"], 1)
        self.assertFalse(d["additive_wall_clock_or_per_query_cost_proven"])
        self.assertFalse(d["live_cli_2_27_2_compatibility_proven"])

    def test_pretty_mixed_variants(self):
        d = self.valid(payload(event(), event("COMPUTE_RECURSIVE"),
                               event("IN_LAYER"), pretty=True))
        self.assertEqual(d["records_checked"], 3)
        self.assertEqual(d["compute_simple_count"], 1)

    def test_anonymized_no_names_or_paths(self):
        d = self.valid(payload(event(predicateName="TOKEN SECRET /private/path.rs")))
        for forbidden in ("TOKEN", "SECRET", "/private/", "predicateName"):
            self.assertNotIn(forbidden, json.dumps(d))

    def test_top_five_only_descending(self):
        entries = [event(millis=v) for v in (7, 5, 3, 1, 9, 2, 8)]
        self.assertEqual(self.valid(payload(*entries))
                         ["compute_simple_largest_five_millis_anonymous"], [9, 8, 7, 5, 3])

    def test_other_strategies_accepted_without_cost_claim(self):
        entries = [event(), *(event(s) for s in sorted(p.STRATEGIES - {"COMPUTE_SIMPLE"}))]
        self.assertEqual(self.valid(payload(*entries))["records_checked"], len(p.STRATEGIES))

    def test_recursion_millis_not_added_to_simple(self):
        self.assertEqual(self.valid(payload(event(), event("COMPUTE_RECURSIVE", millis=10000)))
                         ["compute_simple_largest_five_millis_anonymous"], [120.25])

    def test_missing_simple_fail_closed(self):
        self.fails(payload(event("COMPUTE_RECURSIVE")))

    def test_unknown_strategy_fail_closed(self):
        self.fails(payload(event(), event("QUICK_NEW_STRATEGY")))

    def test_missing_strategy_fail_closed(self):
        self.fails(payload(event(), {"millis": 8}))

    def test_bool_millis_rejected(self):
        self.fails(payload(event(millis=True)))

    def test_negative_millis_rejected(self):
        self.fails(payload(event(millis=-3)))

    def test_string_millis_rejected(self):
        self.fails(payload(event(millis="120ms")))

    def test_huge_millis_rejected(self):
        self.fails(payload(event(millis=p.MAX_MILLIS + 1)))

    def test_nonfinite_millis_rejected(self):
        self.fails(payload(event(millis=float("nan"))))

    def test_huge_integer_millis_fails_closed(self):
        self.fails(payload(event(millis=10**1000)))

    def test_huge_integer_iteration_fails_closed(self):
        self.fails(payload(event(), event("IN_LAYER", predicateIterationMillis=[10**1000])))

    def test_malformed_json_rejected(self):
        self.fails(payload(event()) + "{private")

    def test_duplicate_millis_key_rejected(self):
        self.fails('{"evaluationStrategy":"COMPUTE_SIMPLE","millis":1,"millis":2}\n')

    def test_missing_iteration_rejected(self):
        self.fails(payload(event(), {"evaluationStrategy": "COMPUTE_RECURSIVE", "millis": 5}))

    def test_invalid_iteration_rejected(self):
        self.fails(payload(event(), event("IN_LAYER",
                                          predicateIterationMillis=[float("inf")])))

    def test_negative_iteration_is_allowed_but_not_summed(self):
        x = self.valid(payload(event(), event("IN_LAYER",
                                               predicateIterationMillis=[-9, 3])))
        self.assertNotIn("iteration", json.dumps(x))

    def test_null_scalar_rejected(self):
        self.fails(payload(event(millis=None)))

    def test_non_object_rejected(self):
        self.fails(payload(event()) + "true\n")

    def test_empty_rejected(self):
        self.fails("")

    def test_max_records_limit(self):
        original = p.MAX_RECORDS
        try:
            p.MAX_RECORDS = 2
            self.fails(payload(event(), event(), event()))
        finally:
            p.MAX_RECORDS = original

    def test_max_bytes_limit(self):
        original = p.MAX_INPUT_BYTES
        try:
            p.MAX_INPUT_BYTES = 6
            self.fails(payload(event()))
        finally:
            p.MAX_INPUT_BYTES = original

    def test_record_byte_limit(self):
        original = p.MAX_RECORD_BYTES
        try:
            p.MAX_RECORD_BYTES = 20
            self.fails(payload(event()))
        finally:
            p.MAX_RECORD_BYTES = original

    def test_cli_success_is_numeric_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "private.log"
            path.write_text(payload(event(predicateName="SECRETPRIVATE")))
            run = subprocess.run([sys.executable, "-S", str(SCRIPT), str(path)],
                                 capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(run.stderr, "")
            self.assertNotIn("SECRETPRIVATE", run.stdout)
            self.assertEqual(json.loads(run.stdout)["compute_simple_count"], 1)

    def test_cli_error_no_private_echo(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "secret.log"
            path.write_text('{"evaluationStrategy":"UNKNOWN", "predicateName":"SENSITIVEPATH"}\n')
            run = subprocess.run([sys.executable, "-S", str(SCRIPT), str(path)],
                                 capture_output=True, text=True)
            self.assertEqual(run.returncode, 2)
            self.assertEqual(run.stdout, "")
            self.assertIn("INCOMPLETE_DO_NOT_USE", run.stderr)
            self.assertNotIn("SENSITIVEPATH", run.stderr)


if __name__ == "__main__":
    unittest.main()
