#!/usr/bin/env bash
set -euo pipefail

# A floating rust-toolchain.toml does not upgrade an already-installed channel.
# Resolve the current stable release explicitly on each CI runner.
rustup update stable --no-self-update

active="$(rustc -Vv)"
stable="$(rustup run stable rustc -Vv)"
if [[ "$active" != "$stable" ]]; then
  echo "::error::active rustc differs from the installed Rust Stable toolchain" >&2
  printf 'active:\n%s\nstable:\n%s\n' "$active" "$stable" >&2
  exit 1
fi

printf '%s\n' "$active"
cargo --version
