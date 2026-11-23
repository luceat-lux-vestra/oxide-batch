#!/usr/bin/env bash
# Install the reviewed upstream cargo-cyclonedx release recorded by EG-DEP-001.
# The protected release-tag workflow runs on ubuntu-latest (Linux x86_64).
# Never execute a downloaded installer; verify the configured archive before extraction.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
resolver="$repo_root/.github/scripts/validate_dependency_update_policy.py"
target="x86_64-unknown-linux-gnu"
version="$(python3 "$resolver" --root "$repo_root" --entry cargo-cyclonedx --field version)"
url="$(python3 "$resolver" --root "$repo_root" --entry cargo-cyclonedx --artifact "$target" --field url)"
expected_sha256="$(python3 "$resolver" --root "$repo_root" --entry cargo-cyclonedx --artifact "$target" --field sha256)"

if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "x86_64" ]; then
  echo "::error::cargo-cyclonedx $version installer requires Linux x86_64" >&2
  exit 1
fi

tmpdir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/cargo-cyclonedx.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT
archive="$tmpdir/cargo-cyclonedx.tar.xz"

curl --fail --location --retry 3 --retry-delay 2 "$url" --output "$archive"
echo "$expected_sha256  $archive" | sha256sum --check --strict

tar -xJf "$archive" -C "$tmpdir"
binary="$tmpdir/cargo-cyclonedx-$target/cargo-cyclonedx"
test -x "$binary"

install_dir="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$install_dir"
install -m 0755 "$binary" "$install_dir/cargo-cyclonedx"
test "$(cargo cyclonedx --version)" = "cargo-cyclonedx $version"
