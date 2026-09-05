#!/usr/bin/env python3
"""Negative fixtures for the GitHub Actions security policy validator."""

from __future__ import annotations

import importlib.util
import tempfile
import textwrap
from pathlib import Path


SCRIPT = Path(__file__).with_name("validate_actions_security.py")
SPEC = importlib.util.spec_from_file_location("validate_actions_security", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("could not load validate_actions_security.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def violations(workflow: str) -> list[str]:
    with tempfile.TemporaryDirectory(prefix="oxide-batch-actions-security-") as tmp:
        root = Path(tmp)
        workflows = root / ".github" / "workflows"
        workflows.mkdir(parents=True)
        (workflows / "fixture.yml").write_text(textwrap.dedent(workflow).lstrip())
        return MODULE.validate(root)


def require_rejection(name: str, workflow: str, needle: str) -> None:
    observed = violations(workflow)
    assert observed, f"{name}: broken fixture unexpectedly passed"
    assert any(needle in item for item in observed), (
        f"{name}: expected diagnostic containing {needle!r}; observed {observed!r}"
    )


def require_pass(name: str, workflow: str) -> None:
    observed = violations(workflow)
    assert not observed, f"{name}: valid fixture rejected: {observed!r}"


PINNED_CHECKOUT = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
PG15 = "926f8799aef36e00001cfe15fba7abbd37d3c5224ea57e4c858e4bb670f10561"
PG18 = "4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
BAD_DIGEST = "0" * 64


require_pass(
    "baseline",
    f"""
    name: valid
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        timeout-minutes: 5
        steps:
          - uses: {PINNED_CHECKOUT}
            with:
              persist-credentials: false
          - uses: actions/dependency-review-action@a1d282b36b6f3519aa1f3fc636f609c47dddb294
    """,
)

require_rejection(
    "mutable action",
    """
    name: mutable
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v7
            with:
              persist-credentials: false
    """,
    "full 40-hex commit SHA",
)

require_rejection(
    "checkout credential persistence",
    f"""
    name: credentials
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: {PINNED_CHECKOUT}
    """,
    "persist-credentials: false",
)

require_rejection(
    "workflow top-level write",
    """
    name: broad
    on: issues
    permissions:
      contents: read
      issues: write
    jobs:
      classify:
        runs-on: ubuntu-latest
        steps:
          - run: echo safe
    """,
    "workflow-top-level write permission",
)

require_rejection(
    "untrusted PR title shell interpolation",
    """
    name: injection
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - run: |
              echo "${{ github.event.pull_request.title }}"
    """,
    "untrusted GitHub/input context",
)

require_rejection(
    "untrusted workflow input program interpolation",
    """
    name: injection
    on:
      workflow_dispatch:
        inputs:
          value:
            required: true
            type: string
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3
            with:
              script: |
                console.log("${{ inputs.value }}")
    """,
    "untrusted GitHub/input context",
)

require_rejection(
    "pull_request_target checkout",
    f"""
    name: target
    on: pull_request_target
    permissions:
      contents: read
    jobs:
      test:
        permissions:
          contents: read
          pull-requests: write
        runs-on: ubuntu-latest
        steps:
          - uses: {PINNED_CHECKOUT}
            with:
              persist-credentials: false
    """,
    "pull_request_target workflow must not checkout",
)

require_rejection(
    "mutable service image",
    """
    name: image
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        services:
          postgres:
            image: postgres:18
        steps:
          - run: echo safe
    """,
    "immutable sha256 digest",
)

require_pass(
    "digest-pinned service image",
    f"""
    name: image
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        services:
          postgres:
            image: postgres:18@sha256:{PG18}
        steps:
          - run: echo safe
    """,
)

require_pass(
    "checked-in postgres matrix digest mapping",
    f"""
    name: image-matrix
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        strategy:
          matrix:
            postgres: ["15", "18"]
        services:
          postgres:
            image: postgres:${{{{ matrix.postgres }}}}@sha256:${{{{ matrix.postgres == '15' && '{PG15}' || matrix.postgres == '18' && '{PG18}' || 'unsupported' }}}} # zizmor: ignore[unpinned-images]
        steps:
          - run: echo safe
    """,
)

require_rejection(
    "postgres matrix digest drift",
    f"""
    name: image-matrix
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        strategy:
          matrix:
            postgres: ["15", "18"]
        services:
          postgres:
            image: postgres:${{{{ matrix.postgres }}}}@sha256:${{{{ matrix.postgres == '15' && '{BAD_DIGEST}' || matrix.postgres == '18' && '{PG18}' || 'unsupported' }}}}
        steps:
          - run: echo safe
    """,
    "exact checked-in PostgreSQL 15/18 digest mapping",
)

require_rejection(
    "postgres matrix digest mapping without bounded matrix",
    f"""
    name: image-matrix
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        strategy:
          matrix:
            postgres: ["15", "16", "18"]
        services:
          postgres:
            image: postgres:${{{{ matrix.postgres }}}}@sha256:${{{{ matrix.postgres == '15' && '{PG15}' || matrix.postgres == '18' && '{PG18}' || 'unsupported' }}}}
        steps:
          - run: echo safe
    """,
    "exact checked-in PostgreSQL 15/18 digest mapping",
)

print("GitHub Actions security policy negative fixtures: PASS")
