# M5 Measurement Evidence

**State:** Active

These files are the retained raw observations for the M5 staged crate
extraction. The contract that requires them is the
[staged crate-extraction contract](../../../architecture/crate-extraction.md);
the gate that consumes them is the
[M5 crate-extraction evidence](../../../project/m5-crate-extraction-evidence.md).

Extraction measurements are **reported, not gated**. A build-time or
binary-size change is recorded and reviewed; it fails no stage unless a later
decision makes a budget binding. Correctness, facade equivalence, and durable
invariance are held by tests, not by these numbers.

## Reports

| File | Point measured |
| --- | --- |
| [`baseline.json`](baseline.json) | The single implementation crate, immediately before stage 1 |
| [`stage-1-core.json`](stage-1-core.json) | After `oxide-batch-core` is extracted |
| [`stage-2-repository.json`](stage-2-repository.json) | After `oxide-batch-repository` is extracted |
| [`stage-3-plan.json`](stage-3-plan.json) | After `oxide-batch-plan` is extracted |

## Document shape

Every report carries the same envelope:

- `environment` — source commit, working-tree cleanliness, `rustc`, host, and
  profile;
- `build_seconds` — clean workspace build, clean facade build, and incremental
  facade rebuild, all with every feature enabled;
- `release_binary_bytes` — the release `oxide-batch-cli` binary;
- `packaged_files` — the file count `cargo package --list` reports per
  publishable crate;
- `workspace_dependency_edges` — the crate-level graph, which is the executable
  form of the contract's "module dependency graph before and after the move".

## Reproducing

```bash
docs/engineering/measurements/m5/capture-extraction.sh <stage-label>
```

The script runs `cargo clean` twice, so a capture takes minutes and must not
run concurrently with other cargo work. Durations depend on the host and on
concurrent load and moved by tens of percent between captures of the same
commit; the dependency edges and packaged file counts do not.

Captures were taken on a development macOS host, which the
[support matrix](../../../release/support-matrix.md) lists as development-only.
They are development observations, not release-blocking measurements on the
supported Linux target.
