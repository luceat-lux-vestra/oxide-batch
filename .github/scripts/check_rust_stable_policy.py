#!/usr/bin/env python3
"""Fail-closed static guard for the floating Stable / no-declared-MSRV policy."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


def check_manifest(content: str, where: str) -> list[str]:
    manifest = tomllib.loads(content)
    errors = []
    if "rust-version" in manifest.get("package", {}):
        errors.append(f"{where}: package.rust-version must not be declared")
    if "rust-version" in manifest.get("workspace", {}).get("package", {}):
        errors.append(f"{where}: workspace.package.rust-version must not be declared")
    return errors


def check_toolchain(content: str) -> list[str]:
    channel = tomllib.loads(content).get("toolchain", {}).get("channel")
    return [] if channel == "stable" else [f"rust-toolchain.toml: expected stable, got {channel!r}"]


def check_cache_scripts(consumer: str, producer: str, identity: str) -> list[str]:
    errors = []
    use = 'toolchain_hash="$(bash .github/scripts/rust-cache-identity.sh)"'
    if consumer.count(use) != 6:
        errors.append("ci.yml: all six compiler-cache consumers must use the shared identity helper")
    if producer.count(use) != 1:
        errors.append("sccache-main-prime.yml: producer must use the shared identity helper")
    if 'rustc -Vv' not in identity or "sha256sum rust-toolchain.toml" not in identity:
        errors.append("cache identity must hash both resolved rustc -Vv and toolchain.toml")
    for required in (
        ".github/dependency-update-lifecycle.json",
        "entries['sccache']['pin']['version']",
        "entries['mold']['pin']['version']",
    ):
        if required not in identity:
            errors.append(f"cache identity must include lifecycle tool identity: {required}")
    if re.search(r'toolchain_hash="\$\(sha256sum rust-toolchain\.toml', consumer + producer):
        errors.append("old cache key hashes only the static toolchain file")
    return errors


def self_test() -> None:
    for content in (
        '[package]\nname = "bad"\nversion = "0.1.0"\nrust-version = "1.95"\n',
        '[workspace.package]\nrust-version = "1.95"\n',
    ):
        if not check_manifest(content, "mutation"):
            raise RuntimeError("numeric MSRV mutation was not rejected")
    if not check_toolchain('[toolchain]\nchannel = "1.97.1"\n'):
        raise RuntimeError("numeric Rust toolchain mutation was not rejected")
    if not check_cache_scripts("", "", ""):
        raise RuntimeError("cache key removal mutation was not rejected")

    # A Stable release change must change the cache key even with the exact
    # same rust-toolchain.toml bytes and Cargo.lock.
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory() as tmp:
        executable = Path(tmp) / "rustc"
        keys = []
        for version in ("1.99.0", "1.100.0"):
            executable.write_text(
                "#!/bin/sh\n"
                f"printf 'rustc {version}\\nrelease: {version}\\n"
                f"commit-hash: deadbeef{version}\\n"
                "host: aarch64-unknown-linux-gnu\\n'\n"
            )
            executable.chmod(0o755)
            env = {**os.environ, "PATH": tmp + os.pathsep + os.environ["PATH"]}
            key = subprocess.run(
                ["bash", ".github/scripts/rust-cache-identity.sh"],
                cwd=root, env=env, check=True, capture_output=True, text=True,
            ).stdout.strip()
            if not re.fullmatch(r"[0-9a-f]{64}", key):
                raise RuntimeError("cache identity script emitted invalid digest")
            keys.append(key)
        if keys[0] == keys[1]:
            raise RuntimeError("new Rust Stable release did not rotate cache key")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()

    root = Path(__file__).resolve().parents[2]
    workspace = root / "Cargo.toml"
    root_manifest = tomllib.loads(workspace.read_text())
    errors = check_manifest(workspace.read_text(), "Cargo.toml")
    for member in root_manifest["workspace"]["members"]:
        path = root / member / "Cargo.toml"
        if not path.is_file():
            errors.append(f"{member}: missing Cargo manifest")
            continue
        errors.extend(check_manifest(path.read_text(), str(path.relative_to(root))))

    errors.extend(check_toolchain((root / "rust-toolchain.toml").read_text()))
    clippy = root / "clippy.toml"
    if clippy.is_file() and "msrv" in tomllib.loads(clippy.read_text()):
        errors.append("clippy.toml: numeric MSRV is forbidden")

    workflow = (root / ".github/workflows/ci.yml").read_text()
    prime = (root / ".github/workflows/sccache-main-prime.yml").read_text()
    identity = (root / ".github/scripts/rust-cache-identity.sh").read_text()
    errors.extend(check_cache_scripts(workflow, prime, identity))
    if not re.search(r"(?m)^  msrv:\s*$", workflow) or not re.search(r"(?m)^    name: msrv\s*$", workflow):
        errors.append("legacy required msrv job/context must remain named and executable")
    if "cargo +stable check --workspace --all-targets --all-features --locked" not in workflow:
        errors.append("legacy msrv check must perform a full current Stable workspace check")
    if "cargo +1.95.0" in workflow or "rustup toolchain install 1.95.0" in workflow:
        errors.append("obsolete numeric MSRV compiler commands remain")
    if workflow.count("bash .github/scripts/ensure-stable-rust.sh") < 15:
        errors.append("ordinary Rust CI jobs must resolve current Stable before cargo")
    if prime.count("bash .github/scripts/ensure-stable-rust.sh") != 1:
        errors.append("protected-main cache producer must resolve current Stable")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("PASS: current Rust Stable, no declared numeric MSRV, and compiler-cache parity")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
