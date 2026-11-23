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
  exit 2
fi

: "${RUNNER_TEMP:?GitHub runner temporary directory is required}"
: "${GITHUB_PATH:?GitHub PATH file is required}"
work_dir="$(mktemp -d "$RUNNER_TEMP/cargo-cyclonedx-install.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
archive="$work_dir/cargo-cyclonedx-$target.tar.xz"
extract_root="$work_dir/extracted"
install_root="$RUNNER_TEMP/cargo-cyclonedx-$version"
bin_dir="$install_root/bin"

curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --retry-delay 2 \
  "$url" \
  --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict

mkdir -p "$extract_root"
tar --extract --xz --file "$archive" --directory "$extract_root"

mapfile -t binaries < <(find "$extract_root" -type f -name cargo-cyclonedx -print)
if [ "${#binaries[@]}" -ne 1 ]; then
  echo "::error::Expected exactly one cargo-cyclonedx binary; found ${#binaries[@]}" >&2
  exit 3
fi

mkdir -p "$bin_dir"
install -m 0755 "${binaries[0]}" "$bin_dir/cargo-cyclonedx"
version_output="$(PATH="$bin_dir:$PATH" cargo cyclonedx --version)"
if [[ "$version_output" != *"$version"* ]]; then
  echo "::error::Expected cargo-cyclonedx version $version, observed: $version_output" >&2
  exit 4
fi

echo "$bin_dir" >> "$GITHUB_PATH"
echo "Verified official cargo-cyclonedx $version ($target): $version_output"
