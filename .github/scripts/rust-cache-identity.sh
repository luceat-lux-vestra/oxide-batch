#!/usr/bin/env bash
set -euo pipefail

# Hash both the checked-in toolchain contract and the resolved compiler.
# The literal 'stable' does not change when a new Rust release is installed.
test -f rust-toolchain.toml
toolchain_sha="$(sha256sum rust-toolchain.toml | awk '{print $1}')"
rustc_identity="$(rustc -Vv)"
[[ "$toolchain_sha" =~ ^[0-9a-f]{64}$ ]]
[[ "$rustc_identity" == *"release: "* ]]
[[ "$rustc_identity" == *"commit-hash: "* ]]

printf '%s\n%s\n' "$toolchain_sha" "$rustc_identity" | sha256sum | awk '{print $1}'
