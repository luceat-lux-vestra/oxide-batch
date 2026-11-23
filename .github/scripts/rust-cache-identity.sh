#!/usr/bin/env bash
set -euo pipefail

# Hash the checked-in Rust policy, the resolved compiler, and cache-affecting
# pinned build tools. The literal 'stable' and reviewed tool registry remain
# readable policy declarations while this digest rotates on actual changes.
test -f rust-toolchain.toml
test -f .github/dependency-update-lifecycle.json
toolchain_sha="$(sha256sum rust-toolchain.toml | awk '{print $1}')"
rustc_identity="$(rustc -Vv)"
[[ "$toolchain_sha" =~ ^[0-9a-f]{64}$ ]]
[[ "$rustc_identity" == *"release: "* ]]
[[ "$rustc_identity" == *"commit-hash: "* ]]

runner_arch="${RUNNER_ARCH:-}"
if [ -z "$runner_arch" ]; then
  case "$(uname -m)" in
    aarch64) runner_arch=ARM64 ;;
    x86_64) runner_arch=X64 ;;
    *) echo "unsupported cache architecture: $(uname -m)" >&2; exit 2 ;;
  esac
fi

tool_identity="$(python3 - "$runner_arch" <<'PY'
import json
import sys

runner_arch = sys.argv[1]
data = json.load(open(".github/dependency-update-lifecycle.json", encoding="utf-8"))
entries = {entry["id"]: entry for entry in data["entries"]}
parts = [f"sccache={entries['sccache']['pin']['version']}"]
if runner_arch == "ARM64":
    parts.append(f"mold={entries['mold']['pin']['version']}")
elif runner_arch != "X64":
    raise SystemExit(f"unsupported RUNNER_ARCH: {runner_arch}")
print("\n".join(parts))
PY
)"

printf '%s\n%s\n%s\n' "$toolchain_sha" "$rustc_identity" "$tool_identity" \
  | sha256sum | awk '{print $1}'
