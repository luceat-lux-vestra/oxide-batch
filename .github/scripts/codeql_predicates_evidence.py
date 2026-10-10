#!/usr/bin/env python3
"""Private offline fail-closed metrics from CodeQL predicates summaries.

Not a security scan, authoritative evidence, or an end-to-end wall-time measure.
Never echo predicate identities, names, source paths, or input record contents.
"""
from __future__ import annotations

import argparse
from collections import Counter
import heapq
import json
import math
from pathlib import Path
import sys

MAX_RECORD_BYTES = 2 * 1024 * 1024
MAX_RECORDS = 100_000
MAX_INPUT_BYTES = 512 * 1024 * 1024
READ_BYTES = 32 * 1024
MAX_MILLIS = 1_000_000_000
TOP_COUNT = 5
STRATEGIES = frozenset({
    "COMPUTE_SIMPLE", "COMPUTE_RECURSIVE", "IN_LAYER", "NAMED_LOCAL",
    "COMPUTED_EXTENSIONAL", "EXTENSIONAL", "SENTINEL_EMPTY", "CACHACA",
    "CACHE_HIT",
})


class IncompleteEvidence(ValueError):
    """Only static error codes, never input-dependent messages."""


def _object_pairs(pairs: list[tuple[str, object]]) -> dict:
    output: dict = {}
    for key, value in pairs:
        if key in output:
            raise IncompleteEvidence("duplicate_json_key")
        output[key] = value
    return output


def _nonfinite(_):
    raise IncompleteEvidence("nonfinite_json")


DECODER = json.JSONDecoder(object_pairs_hook=_object_pairs, parse_constant=_nonfinite)


def _objects(reader):
    """Decode both pretty JSON blocks and minified JSON lines, with size caps."""
    pending = ""
    total_bytes = 0
    count = 0
    while True:
        chunk = reader.read(READ_BYTES)
        if chunk:
            total_bytes += len(chunk.encode("utf-8"))
            if total_bytes > MAX_INPUT_BYTES:
                raise IncompleteEvidence("input_limit")
            pending += chunk
        elif not pending.strip():
            break
        pending = pending.lstrip()
        while pending:
            try:
                record, end = DECODER.raw_decode(pending)
            except json.JSONDecodeError:
                if not chunk:
                    raise IncompleteEvidence("malformed_or_truncated_json") from None
                if len(pending.encode("utf-8")) > MAX_RECORD_BYTES:
                    raise IncompleteEvidence("record_limit")
                break
            if len(pending[:end].encode("utf-8")) > MAX_RECORD_BYTES:
                raise IncompleteEvidence("record_limit")
            count += 1
            if count > MAX_RECORDS:
                raise IncompleteEvidence("record_count_limit")
            yield record
            pending = pending[end:].lstrip()
        if not chunk:
            if pending:
                raise IncompleteEvidence("trailing_unparsed_input")
            break


def _millis(value):
    # Bound integers before math.isfinite to avoid huge-int conversion overflow.
    if (type(value) not in (int, float) or not 0 <= value <= MAX_MILLIS
            or not math.isfinite(value)):
        raise IncompleteEvidence("invalid_millis")
    return value


def analyze(reader) -> dict:
    counts = Counter()
    top: list[float] = []
    for record in _objects(reader):
        if type(record) is not dict:
            raise IncompleteEvidence("not_an_object")
        strategy = record.get("evaluationStrategy")
        if type(strategy) is not str or strategy not in STRATEGIES:
            raise IncompleteEvidence("unrecognized_evaluation_strategy")
        counts[strategy] += 1
        if strategy in ("COMPUTE_SIMPLE", "COMPUTE_RECURSIVE"):
            value = _millis(record.get("millis"))
            if strategy == "COMPUTE_SIMPLE":
                heapq.heappush(top, value)
                if len(top) > TOP_COUNT:
                    heapq.heappop(top)
        if strategy in ("COMPUTE_RECURSIVE", "IN_LAYER", "NAMED_LOCAL"):
            iterations = record.get("predicateIterationMillis")
            if type(iterations) is not list or len(iterations) > MAX_RECORDS:
                raise IncompleteEvidence("unverified_iteration_shape")
            # Upstream VS Code source warns negative iteration times can occur.
            # Validate only; never sum, rank or output recursive/SCC timings.
            if any(type(x) not in (int, float) or abs(x) > MAX_MILLIS
                   or not math.isfinite(x) for x in iterations):
                raise IncompleteEvidence("invalid_iteration_millis")
    if counts["COMPUTE_SIMPLE"] == 0:
        raise IncompleteEvidence("missing_simple_predicate_evidence")
    return {
        "classification": "OFFLINE_PRIVATE_PREDICATE_MILLIS_ONLY_NOT_A_SECURITY_SCAN",
        "schema_version": 1,
        "unit": "milliseconds (COMPUTE_SIMPLE.millis only)",
        "records_checked": sum(counts.values()),
        "compute_simple_count": counts["COMPUTE_SIMPLE"],
        "compute_simple_largest_five_millis_anonymous": sorted(top, reverse=True),
        "strategy_counts": {key: counts[key] for key in sorted(STRATEGIES)},
        "additive_wall_clock_or_per_query_cost_proven": False,
        "pinned_cli_version_or_full_query_parity_proven": False,
        "live_cli_2_27_2_compatibility_proven": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("predicates_summary", type=Path,
                        help="PRIVATE local CodeQL predicates summary")
    args = parser.parse_args()
    try:
        if args.predicates_summary.stat().st_size > MAX_INPUT_BYTES:
            raise IncompleteEvidence("input_limit")
        with args.predicates_summary.open(encoding="utf-8") as stream:
            result = analyze(stream)
    except (OSError, UnicodeError, RecursionError):
        print("INCOMPLETE_DO_NOT_USE: invalid_or_unreadable_input", file=sys.stderr)
        return 2
    except IncompleteEvidence as error:
        print("INCOMPLETE_DO_NOT_USE: " + str(error), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
