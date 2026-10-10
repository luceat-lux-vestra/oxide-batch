#!/usr/bin/env python3
"""Opt-in OFFLINE Rust CodeQL evaluator profiler; never an Actions authority.

Requires a pre-existing finalized Rust CodeQL database created using the same
analysis configuration. Never creates a DB, calls GitHub, or uploads results.
"""
from __future__ import annotations

import argparse
import hashlib
import math
import json
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path

EXPECTED_VERSION = "2.27.2"
EXPECTED_PACK = "codeql/rust-queries/0.1.44"
EXPECTED_QUERIES = frozenset('''
queries/diagnostics/AstConsistencyCounts.qlx
queries/diagnostics/CfgConsistencyCounts.qlx
queries/diagnostics/DataFlowConsistencyCounts.qlx
queries/diagnostics/ExtractedFiles.qlx
queries/diagnostics/ExtractionErrors.qlx
queries/diagnostics/ExtractionWarnings.qlx
queries/diagnostics/SsaConsistencyCounts.qlx
queries/diagnostics/TypeInferenceConsistencyCounts.qlx
queries/diagnostics/UnextractedElements.qlx
queries/diagnostics/UnresolvedMacroCalls.qlx
queries/security/CWE-020/RegexInjection.qlx
queries/security/CWE-022/TaintedPath.qlx
queries/security/CWE-078/CommandInjection.qlx
queries/security/CWE-079/XSS.qlx
queries/security/CWE-089/SqlInjection.qlx
queries/security/CWE-295/DisabledCertificateCheck.qlx
queries/security/CWE-311/CleartextTransmission.qlx
queries/security/CWE-312/CleartextLogging.qlx
queries/security/CWE-312/CleartextStorageDatabase.qlx
queries/security/CWE-319/UseOfHttp.qlx
queries/security/CWE-327/BrokenCryptoAlgorithm.qlx
queries/security/CWE-327/WeakSensitiveDataHashing.qlx
queries/security/CWE-614/InsecureCookie.qlx
queries/security/CWE-770/UncontrolledAllocationSize.qlx
queries/security/CWE-798/HardcodedCryptographicValue.qlx
queries/security/CWE-825/AccessInvalidPointer.qlx
queries/security/CWE-918/RequestForgery.qlx
queries/summary/LinesOfCode.qlx
queries/summary/LinesOfUserCode.qlx
queries/summary/NodesWithTypeAtLengthLimit.qlx
queries/summary/NumberOfFilesExtractedWithErrors.qlx
queries/summary/NumberOfSuccessfullyExtractedFiles.qlx
queries/summary/QuerySinkCounts.qlx
queries/summary/SummaryStats.qlx
queries/summary/SummaryStatsReduced.qlx
queries/telemetry/DatabaseQualityDiagnostics.qlx
queries/telemetry/ExtractorInformation.qlx
'''.strip().splitlines())
LOADED = re.compile(r"\[(\d+)/(\d+)\] Loaded .*?/qlpacks/(codeql/rust-queries/[^/]+)/(queries/[^\s]+\.qlx)\.")
EVALUATED = re.compile(r"\[(\d+)/(\d+) eval [^\]]+\] Evaluation done;")


# Privacy boundary: never print unvetted CodeQL summary strings or file paths.
SAFE_METRIC_KEYS = frozenset('''
time totalTime wallTime cpuTime evaluationTime elapsedTime executionTime
queryTime stageTime duration totalDuration evaluationDuration elapsedDuration
executionDuration latency wallClockTime timeMs timeNs timeMillis
wallTimeMs cpuTimeMs elapsedMs elapsedSeconds elapsedMilliseconds
executionTimeMs evaluationTimeMs evaluationTimeSeconds totalTimeMs
totalTimeSeconds durationMs durationNs durationMillis durationSeconds
wallTimeNs cpuTimeNs totalDurationMs totalDurationNs
'''.split())
MAX_RANKS = 5
MAX_METRICS_PER_RANK = 8


def _safe_number(value: object) -> bool:
    return (type(value) in (int, float) and math.isfinite(value)
            and 0 <= value <= 10**18)


def _known_query(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.replace('\\', '/')
    for candidate in EXPECTED_QUERIES:
        if normalized == candidate or normalized.endswith('/' + candidate):
            return candidate
    return None


def _sanitized_rank(entry: object) -> dict:
    """Bounded numeric-only summary; drop arbitrary string values."""
    metrics: dict[str, float | int] = {}
    identity: str | None = None

    def visit(node: object, depth: int) -> None:
        nonlocal identity
        if depth > 4 or len(metrics) >= MAX_METRICS_PER_RANK:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                if not isinstance(key, str):
                    continue
                if key in SAFE_METRIC_KEYS and _safe_number(value):
                    metrics.setdefault(key, value)
                elif isinstance(value, str) and identity is None:
                    identity = _known_query(value)
                elif isinstance(value, (dict, list)):
                    visit(value, depth + 1)
        elif isinstance(node, list):
            for value in node[:MAX_RANKS]:
                if isinstance(value, (dict, list)):
                    visit(value, depth + 1)
    visit(entry, 0)
    result: dict = {'numeric_metrics': dict(sorted(metrics.items()))}
    if identity is not None:
        result['query_id_from_frozen_manifest'] = identity
    return result


def sanitize_overall(summary: object) -> dict:
    """Summarize only explicit numeric performance fields, never raw identifiers.

    The CLI documents 'overall' semantics but not its nested JSON schema.
    On shape changes fail closed rather than inventing timings or leaking data.
    """
    if not isinstance(summary, dict) or not all(
        key in summary for key in ('mostExpensivePerQuery', 'mostExpensivePerStage', 'stats')
    ):
        raise ProfileError('overall summary top-level structure not recognized')
    report = {'classification': 'PRIVATE_AGGREGATES_ONLY_NOT_A_SECURITY_SCAN',
              'schema_version': 1, 'ranks_are_source_order_not_wall_clock_additive': True}
    all_metrics = 0
    for source, target in (('mostExpensivePerQuery', 'query_ranks'),
                           ('mostExpensivePerStage', 'stage_ranks')):
        value = summary[source]
        if isinstance(value, list):
            entries = value
            ordering = 'as_reported_by_codeql'
        elif isinstance(value, dict):
            # Keys may contain source paths. Never echo them or call their
            # iteration order a performance ranking.
            entries = list(value.values()) if all(
                isinstance(v, (dict, list)) for v in value.values()
            ) else [value]
            ordering = 'object_iteration_order_unranked'
        else:
            raise ProfileError(f'unsupported {source} container')
        sanitized = [_sanitized_rank(x) for x in entries[:MAX_RANKS]]
        group_metrics = sum(len(x['numeric_metrics']) for x in sanitized)
        if group_metrics == 0:
            raise ProfileError(f'{source} has no recognized per-item performance metrics')
        all_metrics += group_metrics
        report[target] = {'source_item_count': len(entries), 'order': ordering,
                          'items': sanitized}
    stats = _sanitized_rank(summary['stats'])
    all_metrics += len(stats['numeric_metrics'])
    report['stats_numeric_only'] = stats['numeric_metrics']
    if not all_metrics:
        raise ProfileError('overall summary has no recognized performance metrics')
    return report


class ProfileError(RuntimeError):
    pass


def query_manifest(log: str) -> tuple[set[str], set[int], set[int]]:
    loaded: set[str] = set()
    load_count = 0
    ordinals: set[int] = set()
    evaluated: set[int] = set()
    for match in LOADED.finditer(log):
        number, total, pack, path = match.groups()
        if int(total) != len(EXPECTED_QUERIES) or pack != EXPECTED_PACK:
            raise ProfileError(f"untrusted query pack/count: {pack} {total}")
        if path in loaded or int(number) in ordinals:
            raise ProfileError("duplicate CodeQL loaded query identity or ordinal")
        loaded.add(path)
        ordinals.add(int(number))
        load_count += 1
    for match in EVALUATED.finditer(log):
        if int(match.group(1)) in evaluated:
            raise ProfileError("duplicate CodeQL query evaluation ordinal")
        if int(match.group(2)) != len(EXPECTED_QUERIES):
            raise ProfileError("query evaluation denominator changed")
        evaluated.add(int(match.group(1)))
    if load_count != len(EXPECTED_QUERIES):
        raise ProfileError("unexpected CodeQL loaded query record count")
    return loaded, ordinals, evaluated


def validate_manifest(log: str) -> None:
    loaded, ordinals, evaluated = query_manifest(log)
    expected_ordinals = set(range(1, len(EXPECTED_QUERIES) + 1))
    if loaded != EXPECTED_QUERIES or ordinals != expected_ordinals or evaluated != expected_ordinals:
        raise ProfileError("query parity mismatch (missing/extra/duplicated/misnumbered queries)")


def verify_preflight(codeql: Path, database: Path, output: Path) -> None:
    if not database.is_dir() or not (database / "codeql-database.yml").is_file():
        raise ProfileError("pre-existing CodeQL database with codeql-database.yml required")
    db_meta = (database / "codeql-database.yml").read_text(encoding="utf-8")
    if not re.search(r"(?m)^primaryLanguage:\s*['\"]?rust['\"]?\s*$", db_meta):
        raise ProfileError("pre-existing database is not marked primaryLanguage: rust")
    if not codeql.is_file() or not os.access(codeql, os.X_OK):
        raise ProfileError("explicit executable CodeQL CLI required")
    if output.exists():
        raise ProfileError("output directory must not exist (no clobber)")
    if output == database or output in database.parents or database in output.parents:
        raise ProfileError("output directory must be separate from CodeQL database")
    # Do not place potentially sensitive evaluator event logs into the repo.
    repo_root = Path(__file__).resolve().parents[2]
    if output == repo_root or repo_root in output.parents:
        raise ProfileError("output must be outside the repository worktree")


def run(args: argparse.Namespace) -> dict:
    cli = args.codeql.expanduser().resolve(strict=True)
    db = args.database.expanduser().resolve(strict=True)
    out = args.output_dir.expanduser().resolve(strict=False)
    verify_preflight(cli, db, out)
    version = subprocess.run([str(cli), "version"], text=True, capture_output=True,
                             check=True, timeout=30)
    version_text = version.stdout + version.stderr
    if not re.search(r"(?<!\d)2\.27\.2(?!\d)", version_text):
        raise ProfileError("CodeQL CLI version mismatch: require 2.27.2")
    out.mkdir(mode=0o700, parents=False)
    raw = out / "evaluator-events.json"
    summary = out / "overall-summary.json"
    log_file = out / "run-queries.log"
    cmd = [str(cli), "database", "run-queries", "--threads=4", "--ram=14535",
           "--expect-discarded-cache", "--min-disk-free=1024", "--evaluator-log=" + str(raw),
           "-v", "--", str(db)]
    start = time.monotonic()
    try:
        result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                check=False, timeout=args.timeout_seconds)
        log_file.write_text(result.stdout, encoding="utf-8")
        log_file.chmod(0o600)
        if result.returncode != 0:
            raise ProfileError(f"query run failed (exit={result.returncode})")
        validate_manifest(result.stdout)
        if not raw.is_file() or raw.stat().st_size == 0:
            raise ProfileError("missing/empty structured evaluator event log")
        summary_result = subprocess.run([str(cli), "generate", "log-summary", "--format=overall",
                                         "--utc", "--", str(raw), str(summary)],
                                        text=True, capture_output=True, check=False, timeout=120)
        if summary_result.returncode or not summary.is_file() or summary.stat().st_size == 0:
            raise ProfileError("CodeQL stable log-summary generation failed")
        with summary.open(encoding="utf-8") as stream:
            overall = json.load(stream)
        safe = sanitize_overall(overall)
        safe_path = out / "sanitized-hotspots.json"
        safe_bytes = (json.dumps(safe, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
        # Private file only; the program's stdout never includes these metrics.
        with safe_path.open("xb") as dest:
            dest.write(safe_bytes)
        safe_path.chmod(0o600)
        metrics = {"verdict": "PROFILE_ONLY_QUERY_PARITY_OK_NOT_A_SECURITY_SCAN",
                   "cli_version": EXPECTED_VERSION, "query_pack": EXPECTED_PACK,
                   "query_count": len(EXPECTED_QUERIES), "architecture": platform.machine(),
                   "elapsed_seconds": round(time.monotonic()-start, 3),
                   "database": str(db), "result_artifacts_local_only": True,
                   "sanitized_report_sha256": hashlib.sha256(safe_bytes).hexdigest(),
                   "sanitized_report_schema": 1}
        meta_path = out / "metadata.json"
        meta_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        meta_path.chmod(0o600)
        return metrics
    except Exception:
        (out / "INCOMPLETE_DO_NOT_USE").write_text("Profiling failed; this is not a security gate.\n", encoding="utf-8")
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codeql", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args(argv)
    try:
        if args.timeout_seconds < 1 or args.timeout_seconds > 3600:
            raise ProfileError("timeout must be between 1 and 3600 seconds")
        print(json.dumps(run(args), indent=2))
        return 0
    except (ProfileError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"PROFILE_FAIL_CLOSED: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
