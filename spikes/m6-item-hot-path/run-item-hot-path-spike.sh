#!/usr/bin/env bash
# Reproduces every RFC-0005 measurement in one pass and writes the raw record
# to $OXIDEBATCH_SPIKE_OUT (default target/rfc-0005-spike.json).
#
# The compile-time figures clean only this package before each build, so both
# numbers include the same shared library build and their difference is what
# the extra monomorphization costs.
set -euo pipefail

package="oxide-batch-m6-spikes"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${root}"

output="${OXIDEBATCH_SPIKE_OUT:-${root}/target/rfc-0005-spike.json}"
mkdir -p "$(dirname "${output}")"

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%.0f", time * 1000'
}

echo "== toolchain =="
rustc --version
cargo --version

echo
echo "== equivalence, allocation, dispatch shape, and panic evidence =="
cargo test -p "${package}" --test equivalence
cargo test -p "${package}" --test allocation -- --nocapture
cargo test -p "${package}" --test dispatch_shape
cargo test -p "${package}" --test panic -- --test-threads=1

echo
echo "== compile time and binary size =="
# Each binary gets its own target directory, and the shared library is built
# untimed first, so the recorded time is the bin crate's own codegen — which is
# where the monomorphizations land — and not the dependency graph both share.
#
# Kept to POSIX-style variables rather than associative arrays so the script
# runs on the bash 3.2 that ships with macOS.
scratch="${root}/target/rfc-0005-spike-builds"

measure_build() {
  local bin="$1"
  local dir="${scratch}/${bin}"
  rm -rf "${dir}"
  mkdir -p "${dir}"
  CARGO_TARGET_DIR="${dir}" cargo build --release --quiet -p "${package}" --lib
  local started
  started="$(now_ms)"
  CARGO_TARGET_DIR="${dir}" cargo build --release --quiet -p "${package}" --bin "${bin}"
  echo "$(( $(now_ms) - started )) $(wc -c < "${dir}/release/${bin}" | tr -d ' ')"
}

read -r typed_one_ms typed_one_bytes <<<"$(measure_build size-typed-1)"
echo "size-typed-1:  build_ms=${typed_one_ms} bytes=${typed_one_bytes}"
read -r boxed_one_ms boxed_one_bytes <<<"$(measure_build size-boxed-1)"
echo "size-boxed-1:  build_ms=${boxed_one_ms} bytes=${boxed_one_bytes}"
read -r typed_build_ms typed_bytes <<<"$(measure_build size-typed)"
echo "size-typed:    build_ms=${typed_build_ms} bytes=${typed_bytes}"
read -r boxed_build_ms boxed_bytes <<<"$(measure_build size-boxed)"
echo "size-boxed:    build_ms=${boxed_build_ms} bytes=${boxed_bytes}"

# Marginal cost of one more pipeline, over the fifteen the wide binaries add.
echo "marginal bytes per pipeline: typed=$(( (typed_bytes - typed_one_bytes) / 15 )) boxed=$(( (boxed_bytes - boxed_one_bytes) / 15 ))"

echo
echo "== throughput and allocation record =="
cargo build --release --quiet -p "${package}" --bin measure
measure_json="$(./target/release/measure)"
echo "${measure_json}"

{
  echo "{"
  echo "  \"rustc\": \"$(rustc --version)\","
  echo "  \"build\": {"
  echo "    \"one_pipeline\": {"
  echo "      \"typed\": {\"build_ms\": ${typed_one_ms}, \"bytes\": ${typed_one_bytes}},"
  echo "      \"boxed\": {\"build_ms\": ${boxed_one_ms}, \"bytes\": ${boxed_one_bytes}}"
  echo "    },"
  echo "    \"sixteen_pipelines\": {"
  echo "      \"typed\": {\"build_ms\": ${typed_build_ms}, \"bytes\": ${typed_bytes}},"
  echo "      \"boxed\": {\"build_ms\": ${boxed_build_ms}, \"bytes\": ${boxed_bytes}}"
  echo "    },"
  echo "    \"marginal_bytes_per_pipeline\": {"
  echo "      \"typed\": $(( (typed_bytes - typed_one_bytes) / 15 )),"
  echo "      \"boxed\": $(( (boxed_bytes - boxed_one_bytes) / 15 ))"
  echo "    }"
  echo "  },"
  echo "  \"measure\":"
  echo "${measure_json}" | sed 's/^/  /'
  echo "}"
} > "${output}"

echo
echo "raw evidence written to ${output}"
