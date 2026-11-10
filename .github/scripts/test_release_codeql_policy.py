#!/usr/bin/env python3
"""Fail-closed wiring assertions for the exact-SHA release CodeQL boundary."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
workflows = ROOT / ".github/workflows"
codeql = (workflows / "codeql.yml").read_text()
draft = (workflows / "release-draft.yml").read_text()
release = (workflows / "release.yml").read_text()
ci = (workflows / "ci.yml").read_text()
pr_ci = (workflows / "pr-ci.yml").read_text()
adapter = (ROOT / ".github/scripts/trusted-codeql-runtime-validation.py").read_text()
gate = (ROOT / ".github/scripts/check_release_codeql.py").read_text()
verdict = (ROOT / ".github/scripts/check_codeql_sarif.py").read_text()

def require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)

require("pull_request:" not in codeql and "workflow_run:" not in codeql,
        "ordinary PR CodeQL must stay retired")
require('cron: "47 18 * * 1"' in codeql and "candidate_sha:" in codeql,
        "weekly/manual CodeQL triggers missing")
require("CodeQL schedule guard" in codeql and "should_scan=false" in codeql,
        "unchanged-main schedule guard missing")
require("Not a protected-main ancestor" in codeql
        and "merge-base --is-ancestor" in codeql,
        "historical candidate ancestry proof missing")
for language in ("actions", "rust"):
    require(f"- language: {language}" in codeql, f"{language} full scan missing")
require("release-codeql-source-" in codeql and "source-identity.json" in codeql,
        "source provenance manifest missing")
require(codeql.count("retention-days: 30") == 2,
        "SARIF/source evidence must use the repository 30-day retention ceiling")
require("Rust security query rule set was reduced" in verdict
        and "RUST_REQUIRED_SECURITY_RULES" in verdict
        and "expected_source_files" in verdict
        and "extraction warning" in verdict,
        "Rust query/extraction fail-closed verdict missing")
require("queries:" not in codeql,
        "CodeQL workflow must not narrow the default query suite")
for token in ("Dispatched trusted full CodeQL", "Joining active exact-source CodeQL",
              "timed out waiting", "ambiguous active exact-source CodeQL scans"):
    require(token in gate, f"release CodeQL state machine missing {token}")

require("  codeql-preflight:" in draft and "actions: write" in draft
        and "contents: read" in draft,
        "release-draft scanner authority is not isolated")
require("needs: codeql-preflight" in draft
        and "always() && (github.event_name == 'workflow_dispatch' || needs.codeql-preflight.result == 'success')" in draft,
        "draft mutation/recovery job is not correctly gated on CodeQL preflight")
require(draft.index("Require exact-tag-SHA CodeQL evidence before new draft mutations")
        < draft.index("  prepare:")
        < draft.index("- name: Attest package provenance")
        < draft.index("- name: Create or refresh draft release"),
        "draft publication ordering is not fail-closed")
require(draft.count("actions: write") == 1,
        "draft workflow must grant scan dispatch authority only to the preflight job")

require("  codeql-preflight:" in release and "actions: write" in release,
        "registry scanner authority is not isolated")
require("needs: [verify, codeql-preflight]" in release
        and "needs.codeql-preflight.result == 'success'" in release,
        "registry publisher is not gated on CodeQL preflight")
require(release.index("  verify:") < release.index("  codeql-preflight:")
        < release.index("  publish:"),
        "registry preflight must run after read-only classification and before publication")
require(release.index("Enforce exact-tag-SHA CodeQL before registry authentication")
        < release.index("Authenticate to crates.io")
        < release.index("Publish to crates.io"),
        "CodeQL must precede OIDC authentication and crates.io writes")
require(release.count("actions: write") == 1,
        "registry workflow must grant scan dispatch authority only to preflight")
require("actions: write" not in release[release.index("  publish:"):],
        "publisher must not hold CodeQL dispatch authority")

require("check_codeql_sarif.py --self-test" in ci
        and "check_release_codeql.py --self-test" in ci
        and "test_release_codeql_policy.py" in ci,
        "PR CI must exercise SARIF, lifecycle and publication-order regressions")
require("  codeql-runtime:" in pr_ci
        and "github/codeql-action/init@" in pr_ci
        and "github/codeql-action/analyze@" in pr_ci
        and "upload: never" in pr_ci
        and "trusted-codeql-runtime-validation.py verify" in pr_ci,
        "selective PR CodeQL runtime adapter wiring missing")
require("production_evidence" in adapter and "pull_request" in adapter and "metadata_ok" in adapter and "check_codeql_sarif.py" in adapter,
        "PR CodeQL runtime must prove local verdict and production-evidence rejection")
require("upload-artifact" not in pr_ci[pr_ci.index("  codeql-runtime:"):pr_ci.index("  direct-merge-gate:")],
        "PR CodeQL runtime SARIF must not be retained as an artifact")

print("Oxide exact-SHA CodeQL release boundary policy passed")
