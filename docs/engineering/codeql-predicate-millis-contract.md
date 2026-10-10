# CodeQL P9: offline predicate-millisecond evidence contract

**Status:** opt-in PRIVATE utility, synthetic-tested only. NOT a CodeQL security scan, SARIF result, merge authority or performance optimization. Owner: oxide-batch #433. Dependent Arkst optimization: #585.

## Published source contracts

- The official GitHub CodeQL CLI manual describes 'generate log-summary --format=predicates' as a stream of JSON predicate summaries, whereas '--format=overall' is overall statistics and expensive evaluations. It does NOT publish the deep shapes and numeric units of 'mostExpensivePerQuery'/'mostExpensivePerStage' in pinned CodeQL CLI 2.27.2.
  https://docs.github.com/en/code-security/reference/code-scanning/codeql/codeql-cli-manual/generate-log-summary
- GitHub's public CodeQL VS Code source types contain 'COMPUTE_SIMPLE.millis' and 'COMPUTE_RECURSIVE.millis':
  https://github.com/github/vscode-codeql/blob/f90cb50289846e36319edeff7d859726f0f98d91/extensions/ql-vscode/src/log-insights/log-summary.ts
- The same source explicitly interprets 'COMPUTE_SIMPLE.millis' as milliseconds but warns that 'COMPUTE_RECURSIVE.millis' is **the SCC total, not exclusive predicate cost**. Other strategies use positive iteration times as **best-effort estimates**; negative iteration values can exist. Do NOT sum or rank these as authoritative end-to-end wall time:
  https://github.com/github/vscode-codeql/blob/f90cb50289846e36319edeff7d859726f0f98d91/extensions/ql-vscode/src/log-insights/performance-comparison.ts

These client source definitions are **not a proven live schema match for CodeQL CLI 2.27.2**. The first P8 ARM64 real schema probe produced partial 'overall' structure only and did not validate its unknown deep numeric fields.

## Offline use and privacy

An operator with a PRIVATE, isolated evaluator event log may run, offline and outside any protected CodeQL DB:

    codeql generate log-summary --format=predicates --utc -- /private/evaluator-events.json /private/predicates-summary.jsonl
    python3 .github/scripts/test_codeql_predicates_evidence.py
    python3 .github/scripts/codeql_predicates_evidence.py /private/predicates-summary.jsonl

The helper performs no GitHub/network calls, CodeQL invocation, DB writes or uploads. Its stdout is fixed-field numeric-only JSON: counts per documented evaluation strategy plus the five largest ANONYMOUS 'COMPUTE_SIMPLE.millis' values. No predicate names, source paths, strings, query IDs, RA hashes or SCC timings appear in output. No metrics are added into an alleged query time or wall time. Unrecognized data, malformed structure, unknown strategy, duplicate JSON key, nonfinite/bool/negative simple millis, absent simple computation or exceeded resource budgets result in 'INCOMPLETE_DO_NOT_USE' and **no success JSON**.

Raw evaluator logs and unfiltered predicates summaries may contain sensitive names/paths/data. Retain them privately: never attach, upload, commit or output them in GitHub Actions. Do not reuse or modify authoritative CodeQL security databases, configuration, 37/37 query manifest, SARIF or Merge Gate.

## Validation / next P10 gate

**29/29 adversarial mock-only tests** and Python compilation prove the offline parser's fixed numeric output, bounded decoding and refusal behavior on synthetic data. They **do not** prove compatibility with CodeQL CLI 2.27.2 live predicates streams or any scan-speed improvement. A later isolated P10 experiment must first prove actual 2.27.2 event shape, 37-query pack/manifest parity, source coverage and errors, runner/CLI identity and private stdout. If the shape differs, keep FAIL-CLOSED; do not expand numeric allowlists without evidence. Do not treat anonymous predicate durations as causally identifying Arkst #585's observed 13-to-14 query-completion gap.
