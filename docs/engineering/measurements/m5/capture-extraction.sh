#!/usr/bin/env bash
# Captures the build-time, binary-size, packaging, and dependency-graph
# observations the staged crate-extraction contract requires per stage.
#
# Usage: docs/engineering/measurements/m5/capture-extraction.sh <stage-label>
#
# The label names the point being measured, for example `baseline` or
# `stage-1-core`. Output is written to
# `docs/engineering/measurements/m5/<label>.json`.
#
# These are provisional observations under the performance plan. A build-time
# or binary-size change is reported, not gated.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <stage-label>" >&2
  exit 2
fi

label="$1"
root="$(cd "$(dirname "$0")/../../../.." && pwd)"
output="${root}/docs/engineering/measurements/m5/${label}.json"

cd "${root}"

commit="$(git rev-parse HEAD)"
if [ -z "$(git status --porcelain)" ]; then
  clean_tree="true"
else
  clean_tree="false"
fi
rustc_version="$(rustc --version)"
host="$(uname -s)-$(uname -m)"

# Wall-clock seconds for one command, with output discarded.
elapsed() {
  local start end
  start="$(date +%s.%N)"
  "$@" >/dev/null 2>&1
  end="$(date +%s.%N)"
  awk -v s="${start}" -v e="${end}" 'BEGIN { printf "%.1f", e - s }'
}

echo "==> clean workspace build" >&2
cargo clean
clean_workspace="$(elapsed cargo build --workspace --all-features)"

echo "==> incremental facade build" >&2
touch crates/oxide-batch/src/lib.rs
incremental_facade="$(elapsed cargo build --package oxide-batch --all-features)"

echo "==> clean facade build" >&2
cargo clean
clean_facade="$(elapsed cargo build --package oxide-batch --all-features)"

echo "==> release operator CLI" >&2
cargo build --release --package oxide-batch-cli >/dev/null 2>&1
binary="target/release/oxide-batch"
if [ -f "${binary}" ]; then
  binary_bytes="$(wc -c <"${binary}" | tr -d ' ')"
else
  binary_bytes="null"
fi

echo "==> packaged file counts" >&2
# `cargo package --list` prints one crate at a time, so count per package.
# `--allow-dirty` keeps the observation available on a working tree; the
# release-blocking packaging evidence is `cargo xtask package` on a clean
# checkout.
packaged_json=""
for package in $(cargo metadata --format-version 1 --no-deps |
  python3 -c 'import json,sys; print(" ".join(p["name"] for p in json.load(sys.stdin)["packages"] if p.get("publish") != []))'); do
  files="$(cargo package --package "${package}" --allow-dirty --list 2>/dev/null | grep -c . || echo 0)"
  if [ -n "${packaged_json}" ]; then
    packaged_json="${packaged_json},"
  fi
  packaged_json="${packaged_json}
    \"${package}\": ${files}"
done

echo "==> workspace dependency edges" >&2
edges="$(cargo metadata --format-version 1 --all-features |
  python3 -c '
import json, sys

metadata = json.load(sys.stdin)
members = set(metadata["workspace_members"])
names = dict((p["id"], p["name"]) for p in metadata["packages"])
edges = []
for node in metadata["resolve"]["nodes"]:
    if node["id"] not in members:
        continue
    for dep in node["deps"]:
        if dep["pkg"] in members:
            edges.append(names[node["id"]] + " -> " + names[dep["pkg"]])
print(json.dumps(sorted(set(edges)), indent=6)[1:-1].rstrip())
')"

cat >"${output}" <<JSON
{
  "label": "${label}",
  "environment": {
    "commit": "${commit}",
    "clean_tree": ${clean_tree},
    "rustc": "${rustc_version}",
    "host": "${host}",
    "profile": "dev, plus one release build for the operator CLI"
  },
  "build_seconds": {
    "clean_workspace_all_features": ${clean_workspace},
    "clean_facade_all_features": ${clean_facade},
    "incremental_facade_all_features": ${incremental_facade}
  },
  "release_binary_bytes": {
    "oxide-batch-cli": ${binary_bytes}
  },
  "packaged_files": {${packaged_json}
  },
  "workspace_dependency_edges": [${edges}
  ]
}
JSON

echo "wrote ${output}" >&2
