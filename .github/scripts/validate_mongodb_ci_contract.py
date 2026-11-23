#!/usr/bin/env python3
"""Fail closed if MongoDB runnable-core support loses its required CI proof."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

WORKFLOW = Path(".github/workflows/ci.yml")
INVOCATION = "python3 .github/scripts/validate_mongodb_ci_contract.py --self-test"

REQUIRED_JOB_LITERALS = (
    "mongodb-repository-core:",
    "name: mongodb-${{ matrix.lane }}-repository-core",
    'lane: "8.0"',
    'version: "8.0.38"',
    "mongo:8.0.38@sha256:d97fea17ccb8d004cd137be8dab54d70d0df11be69c9f422aefe4484c8414ab7",
    'lane: "8.2"',
    'version: "8.2.12"',
    "mongo:8.2.12@sha256:e0ce8c35124d4a9f9785532d1f268f39e9728ffa1cb38f46fa482436424c4bd3",
    "runs-on: ubuntu-26.04-arm",
    "if: ${{ github.event.pull_request.draft == false }}",
    "OXIDEBATCH_MONGODB_LANE: ${{ matrix.lane }}",
    "OXIDEBATCH_MONGODB_TEST_URI: mongodb://127.0.0.1:27017/?replicaSet=rs0",
    "OXIDEBATCH_MONGODB_TEST_DATABASE: oxide_batch_repository",
    'OXIDEBATCH_MONGODB_FAILPOINTS: "1"',
    "OXIDEBATCH_MONGODB_EXPECTED_VERSION: ${{ matrix.version }}",
    "--replSet rs0 --bind_ip_all --setParameter enableTestCommands=1",
    'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})',
    "db.hello().isWritablePrimary",
    "cargo test -p oxide-batch --features mongodb",
    "--test mongodb_repository_core",
    "-- --nocapture --test-threads=1",
    "trap cleanup EXIT",
    "Run authenticated MongoDB privilege certification",
    "OXIDEBATCH_MONGODB_AUTH_IMAGE: ${{ matrix.image }}",
    "OXIDEBATCH_MONGODB_AUTH_LANE: ${{ matrix.lane }}",
    "OXIDEBATCH_MONGODB_AUTH_EXPECTED_VERSION: ${{ matrix.version }}",
    "./tests/fixtures/mongodb/run-auth-certification.sh",
    "\"$OXIDEBATCH_MONGODB_AUTH_IMAGE\"",
    "\"$OXIDEBATCH_MONGODB_AUTH_LANE\"",
    "\"$OXIDEBATCH_MONGODB_AUTH_EXPECTED_VERSION\"",
    "Run MongoDB TLS certification",
    "OXIDEBATCH_MONGODB_TLS_IMAGE: ${{ matrix.image }}",
    "OXIDEBATCH_MONGODB_TLS_LANE: ${{ matrix.lane }}",
    "OXIDEBATCH_MONGODB_TLS_EXPECTED_VERSION: ${{ matrix.version }}",
    "./tests/fixtures/mongodb/run-tls-certification.sh",
    "\"$OXIDEBATCH_MONGODB_TLS_IMAGE\"",
    "\"$OXIDEBATCH_MONGODB_TLS_LANE\"",
    "\"$OXIDEBATCH_MONGODB_TLS_EXPECTED_VERSION\"",
    "Run MongoDB backup/restore certification",
    "OXIDEBATCH_MONGODB_BACKUP_IMAGE: ${{ matrix.image }}",
    "OXIDEBATCH_MONGODB_BACKUP_LANE: ${{ matrix.lane }}",
    "OXIDEBATCH_MONGODB_BACKUP_EXPECTED_VERSION: ${{ matrix.version }}",
    "./tests/fixtures/mongodb/run-backup-restore-certification.sh",
    "\"$OXIDEBATCH_MONGODB_BACKUP_IMAGE\"",
    "\"$OXIDEBATCH_MONGODB_BACKUP_LANE\"",
    "\"$OXIDEBATCH_MONGODB_BACKUP_EXPECTED_VERSION\"",
    "--publish 27017:27017",
    'container="oxide-mongodb-$OXIDEBATCH_MONGODB_LANE"',
)

FORBIDDEN_JOB_LITERALS = (
    'container="oxide-mongodb-${{ matrix.lane }}"',
    "          '${{ matrix.image }}'",
    "          '${{ matrix.lane }}'",
    "          '${{ matrix.version }}'",
)


def mongodb_job(source: str) -> str:
    match = re.search(
        r"(?ms)^  mongodb-repository-core:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        source,
    )
    if match is None:
        raise AssertionError("MongoDB runnable-core job is missing")
    return "  mongodb-repository-core:\n" + match.group(1)


def validate(source: str) -> None:
    job = mongodb_job(source)
    for literal in REQUIRED_JOB_LITERALS:
        if job.count(literal) != 1:
            raise AssertionError(
                f"MongoDB runnable-core job literal must appear exactly once: {literal}"
            )
    for literal in FORBIDDEN_JOB_LITERALS:
        if literal in job:
            raise AssertionError(
                f"MongoDB runnable-core job contains forbidden shell interpolation: {literal}"
            )
    if source.count(INVOCATION) != 1:
        raise AssertionError("MongoDB CI contract validator must run exactly once")


def self_test(source: str) -> None:
    validate(source)
    job = mongodb_job(source)
    for literal in REQUIRED_JOB_LITERALS:
        mutated_job = job.replace(literal, "", 1)
        mutated = source.replace(job, mutated_job, 1)
        try:
            validate(mutated)
        except AssertionError:
            continue
        raise AssertionError(f"negative fixture did not reject removal of {literal!r}")

    for literal in FORBIDDEN_JOB_LITERALS:
        if literal.startswith("container="):
            mutated_job = job.replace(
                'container="oxide-mongodb-$OXIDEBATCH_MONGODB_LANE"',
                literal,
                1,
            )
        else:
            env_variable = {
                "          '${{ matrix.image }}'": '"$OXIDEBATCH_MONGODB_AUTH_IMAGE"',
                "          '${{ matrix.lane }}'": '"$OXIDEBATCH_MONGODB_AUTH_LANE"',
                "          '${{ matrix.version }}'": '"$OXIDEBATCH_MONGODB_AUTH_EXPECTED_VERSION"',
            }[literal]
            mutated_job = job.replace(env_variable, literal.strip(), 1)
        mutated = source.replace(job, mutated_job, 1)
        try:
            validate(mutated)
        except AssertionError:
            continue
        raise AssertionError(
            f"negative fixture did not reject forbidden shell interpolation {literal!r}"
        )

    mutated = source.replace(INVOCATION, "", 1)
    try:
        validate(mutated)
    except AssertionError:
        return
    raise AssertionError("negative fixture did not reject validator detachment")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", type=Path, default=WORKFLOW)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    source = args.workflow.read_text(encoding="utf-8")
    if args.self_test:
        self_test(source)
        print("MongoDB runnable-core CI contract self-test: PASS")
    else:
        validate(source)
        print("MongoDB runnable-core CI contract: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
