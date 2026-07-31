# `LIFE-DEFINITION-001` wrapper equivalence traces

**Scenario:** `LIFE-DEFINITION-001` in the
[compatibility ledger](../../../../../docs/compatibility/conformance-matrix.md).

**Source:** independently authored synthetic fixture. No external
specification, production extract, or third-party material is copied here.

**Format version:** 1 — one normalized line per repository command, lifecycle
event, or durable row, in observation order.

**Regeneration:**

```shell
OXIDEBATCH_UPDATE_TRACE_GOLDEN=1 cargo test -p oxide-batch --test plan_equivalence
```

**Seed:** none. Every scenario uses an injected manual clock and a
deterministic identifier sequence, so no randomness participates.

These traces were captured from the one-step wrapper implementation *before*
compiled-plan lowering landed, at commit `1fea043`. They are the reference for
the wrapper-equivalence gate in
[the basic-flow contract](../../../../../docs/architecture/basic-flow.md):
routing a lowered one-step definition through the compiled plan must reproduce
every line unchanged.

A trace records identifiers, statuses, exit codes, optimistic versions, counts,
failure categories, and manifest format/digest prefixes only. Parameters,
contexts, item values, error payloads, and manifest bytes are never recorded.
