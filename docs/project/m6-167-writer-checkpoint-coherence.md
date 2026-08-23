# M6 Flat-File Writer Checkpoint Coherence Correction

**Issue:** #167  
**PR:** #168

This note is the authoritative correction for the #167 writer-concurrency defect model. It supersedes the **Third corrective pass (#167)** wording in `docs/project/m6-147-flat-file-evidence.md` where that older text describes a smaller committed value being assigned after a larger one and the checkpoint therefore regressing backward.

## Actual historical defect

Before #167, both `DelimitedWriter` and `FixedWidthWriter` used two independent synchronization domains:

```text
file:            Arc<Mutex<File>>
committed_bytes: Arc<Mutex<u64>>
```

A successful write performed, in order:

```text
lock file
write_all(buffer)
sync_data()
unlock file
lock committed_bytes
committed_bytes += buffer.len()
unlock committed_bytes
```

The counter update was additive (`saturating_add(buffer.len())`), not an assignment of a precomputed absolute file position. Therefore the precise defect was **not** that two publications could finish out of order and overwrite a larger final value with a smaller one. Once every successful additive publication completed, the final sum could still equal the final file length.

The correctness hole existed in the intermediate state between the two locks. Multiple physical writes could already be durable while only a subset of their additive increments had been published. `ItemStream::update()`, which read only `committed_bytes`, could snapshot such an intermediate value.

That snapshot need not identify any complete physical write-call prefix.

### Delimited example

Force this ordering:

1. write A appends `AAAA\n` (5 bytes), syncs, releases the file lock, then pauses before `committed += 5`;
2. write B appends `B\n` (2 bytes), syncs, then publishes `committed += 2`;
3. `ItemStream::update()` snapshots `committed == 2`;
4. the physical file is already `AAAA\nB\n` (7 bytes).

Offset 2 is inside the first CSV record (`AA`). Restart/open using that checkpoint would truncate the file to an invalid partial record. After A eventually publishes its `+5`, the final counter becomes 7; that later recovery of the numerical total does not make the previously exposed checkpoint safe.

### Fixed-width example

For one-field, one-byte records with `\n` terminators, let write A contain three records (`1\n2\n3\n`, 6 bytes) and write B contain one (`4\n`, 2 bytes). Under the same forced interleaving, `update()` can observe checkpoint 2 while all 8 bytes are already physical.

Offset 2 happens to be a record boundary, but it is **not a complete write-call prefix**: it retains only one record out of A's successful three-record batch and discards the remainder of A plus B on restart. The writer/checkpoint contract is batch-transition coherence, not merely syntactic line alignment.

## Fix invariant

#168 replaces the split state with one private state object per writer:

```text
WriterState {
    file,
    committed_bytes,
}
```

behind one `Arc<Mutex<WriterState>>` shared by the writer and its paired stream.

A successful transition is now:

```text
lock state
write_all(buffer)
sync_data()
candidate = file.stream_position()
committed_bytes = candidate
unlock state
```

`open()` and `update()` use the same state lock. Consequently an observer can see only a state before a write transition or after that transition; it cannot see the physical file after the write while still seeing checkpoint state from before the same transition.

`stream_position()` is the source of truth for the committed absolute position after the synchronized write rather than an independently accumulated byte count.

## Executable evidence

The authoritative #167 regression target is:

`crates/oxide-batch/tests/writer_checkpoint_coherence.rs`

It contains:

- deterministic negative controls that implement the actual historical split-lock/additive algorithm and force the harmful interleaving with synchronous channels, with no sleeps or scheduler-probability assumptions;
- unequal-size delimited and fixed-width batches;
- real public production `write()` calls racing the real `ItemStream::update()` implementation;
- assertions that production checkpoints are only complete serialized write-call prefixes;
- final `committed_bytes == physical file length` checks;
- restart/open using the captured concurrent checkpoint, proving truncation retains an exact complete prefix rather than a partial transition.

The older inline module tests in `delimited.rs` and `fixed_width.rs` remain useful smoke tests for the single-lock serialization shape, but they are not the authoritative reproduction of the historical additive intermediate-state race.

## Scope

This correction changes neither the public API nor the production fix shape. CSV dialect behavior, fixed-width layout behavior, restart codec/schema identity, uncommitted-tail truncation, shorter-than-committed fail-closed behavior, `sync_data()` semantics, and the existing no-directory-fsync durability boundary remain unchanged.

`IO-FLAT-001` remains **Implemented**, not promoted to **Verified** by this corrective pass alone.
