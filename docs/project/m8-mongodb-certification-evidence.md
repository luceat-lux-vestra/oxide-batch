# M8 MongoDB Metadata Repository Certification Evidence

**State:** Accepted main-branch implementation evidence

**Owner:** M8 tracker [#201](https://github.com/luceat-lux-vestra/oxide-batch/issues/201) / MongoDB adapter [#646](https://github.com/luceat-lux-vestra/oxide-batch/issues/646)

**Certification baseline:** main `df490e5e0f5208b83383c648b0c656b2c55481db`

This record closes the implementation-evidence gap for `DB-MONGO-001`. It
records an **Implemented** MongoDB metadata adapter profile on main. It does
not add MongoDB to a released support promise and does not promote the ledger
row to `Verified`; that requires a named OxideBatch release carrying the
claim.

## Certified matrix

| Dimension | Certified value |
| --- | --- |
| MongoDB oldest lane | 8.0.38, `mongo:8.0.38@sha256:d97fea17ccb8d004cd137be8dab54d70d0df11be69c9f422aefe4484c8414ab7` |
| MongoDB newest lane | 8.2.12, `mongo:8.2.12@sha256:e0ce8c35124d4a9f9785532d1f268f39e9728ffa1cb38f46fa482436424c4bd3` |
| Rust MongoDB driver | `mongodb 3.10.1` |
| Metadata schema | 3 |
| Optional repository capabilities | none declared; unsupported optional capabilities fail closed |
| Final aggregate validation | PR #697 exact head `54db4878f07b25c90f004c98a1a80757f9219293`, PR CI run `44346926087`: success |
| Merge baseline | PR #697 squash `df490e5e0f5208b83383c648b0c656b2c55481db` |

The matrix is a first-support profile. MongoDB schema 2 existed only as an
unreleased implementation state and is not a supported migration source.
There is no synthetic 2 -> 3 support claim. The first release-backed schema
change after schema 3 must add the migration and pre-upgrade
backup/restore-only rollback evidence required by the accepted Gate-C policy.

## Support-level core evidence

The frozen Gate-A support-level profile has twelve criteria. The MongoDB
adapter satisfies them as follows:

1. **Schema initialization/current-version readback.** `MongoMigrator`
   initializes schema 3 idempotently and reads the installed marker.
2. **Supported upgrade/newer rejection.** The first-support source set contains
   no release-backed predecessor; undefined older schema 2 fails closed and
   newer schemas are rejected rather than guessed compatible.
3. **Concurrent duplicate launch.**
   `concurrent_launch_creates_one_mongodb_instance_and_attempt` proves one
   logical instance/attempt under concurrent launch.
4. **Durable identity allocation.** Repository-created identities are durable
   and the restore certification proves allocator/counter state advances after
   restore without ID reuse.
5. **Lifecycle CAS/stale rejection.** The shared runnable-core repository
   contract exercises optimistic lifecycle versions and stale-write rejection.
6. **Failed/stopped restart with checkpoint/context.** PR
   [#691](https://github.com/luceat-lux-vestra/oxide-batch/pull/691) persists
   MongoDB checkpoint/context state and PR
   [#692](https://github.com/luceat-lux-vestra/oxide-batch/pull/692) proves
   process-kill restart from committed state.
7. **Crash/disconnect transaction boundaries.** PR
   [#689](https://github.com/luceat-lux-vestra/oxide-batch/pull/689) certifies
   mutation/commit fault classification; the process-kill campaign retains
   fresh-session reconciliation.
8. **Explicit unknown commit outcome.**
   `mongodb_unknown_commit_result_is_never_guessed_rolled_back` requires
   `CommitOutcomeUnknown` instead of inventing rollback.
9. **Bounded Explorer profile.**
   `explorer_contract_passes_on_mongodb` exercises the accepted bounded read
   profile; unsupported Explorer surfaces retain fail-closed behavior.
10. **Resource bounds/timeouts/cancellation.** PR
    [#693](https://github.com/luceat-lux-vestra/oxide-batch/pull/693) proves pool
    saturation/acquire-timeout recovery and PR
    [#694](https://github.com/luceat-lux-vestra/oxide-batch/pull/694) proves
    bounded unavailable-endpoint connect/server-selection failure.
11. **Authentication/privilege/TLS.** PR
    [#695](https://github.com/luceat-lux-vestra/oxide-batch/pull/695) certifies
    scoped runtime privileges and bad-credential failure; PR
    [#696](https://github.com/luceat-lux-vestra/oxide-batch/pull/696) certifies
    CA/server-identity validation and plaintext rejection.
12. **Backup/restore.** PR
    [#697](https://github.com/luceat-lux-vestra/oxide-batch/pull/697) performs
    same-schema-3 dump/restore into a clean isolated target on both pinned
    MongoDB lanes, reconciles identity/lifecycle/checkpoint/context/flow
    decisions/indexes, then performs a compatible restart and fresh mutation.

PRs [#687](https://github.com/luceat-lux-vestra/oxide-batch/pull/687) and
[#688](https://github.com/luceat-lux-vestra/oxide-batch/pull/688) additionally
harden deterministic client teardown and fail-closed schema-inception policy.
The full rolling evidence and exact-head validation checkpoints remain under
[#646](https://github.com/luceat-lux-vestra/oxide-batch/issues/646).

## Backup/restore retained evidence

The final exact-head validation produced successful same-schema backups on both
pinned lanes:

- MongoDB 8.0.38: archive SHA-256
  `9bcbf21191f1b9a9bb79c5e932b9fb1e8082623702258ff00cf4103959f7eacc`,
  8,905 bytes.
- MongoDB 8.2.12: archive SHA-256
  `8a9b1dc8db14f7b32afda14ca430a0ea6594654ada8887233f911660d84dd7ff`,
  8,905 bytes.

The raw archive is intentionally ephemeral. The certification retains the
verdict, exact source/target versions, schema, artifact digest/size, semantic
reconciliation, repository reopen, and post-restore mutation evidence instead
of committing a database dump containing fixture state.

## Capability and support boundary

The MongoDB `RepositoryDescriptor` declares no optional repository
capabilities. This certification therefore does **not** imply support for
definition-registry upgrades, execution ownership/fencing, operator retention,
archive/export, same-resource business writes, or other capability-specific
families. Calls that require undeclared capabilities continue to fail closed.

The earned ledger status is `Implemented`: code and the required main-branch
evidence exist. `Verified` remains unavailable until a named released
OxideBatch version includes MongoDB in its release support matrix and carries
the required evidence. The published 0.6.0 support promise is unchanged.
