#!/usr/bin/env bash
# Install exactly the reviewed upstream cargo-cyclonedx 0.5.9 release binary.
# The protected release-tag workflow runs on ubuntu-latest (Linux x86_64).
# Never execute a downloaded installer; verify the archive before extraction.
set -euo pipefail

version="0.5.9"
target="x86_64-unknown-linux-gnu"
expected_sha256="fb8dbee9f182173e062a64a387b21a0badc6fab8b2abf9294973f012972bf6d8"

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
  "https://github.com/CycloneDX/cyclonedx-rust-cargo/releases/download/cargo-cyclonedx-$version/cargo-cyclonedx-$target.tar.xz" \
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
