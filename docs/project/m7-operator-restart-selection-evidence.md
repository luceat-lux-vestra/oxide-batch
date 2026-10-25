# M7 Gate E: operator restart selection evidence (#613)

**State:** candidate implementation evidence for #613. This is not #199 or M7 exit
acceptance; #200 remains the M7-wide evidence owner.

## Bound

This slice completes only the operator-facing selection between strict restart
and one exact compatible-upgrade edge.

- `OperatorRequest::restart` remains the strict constructor and binds one exact
  `DefinitionIdentity`.
- `OperatorRequest::restart_compatible` binds one durable
  `DefinitionUpgrade`. Its request digest covers source/target definition
  identity, edge key/revision, step and node mappings, and every durable state
  transform descriptor.
- Process-local `DefinitionStateTransformer` code is never hashed or persisted.
  The first compatible application supplies the matching
  `DefinitionUpgradeExecution` separately.
- `JobOperator::execute_with_upgrade` rejects missing or mismatched live
  execution before lifecycle or audit mutation.
- Replay probes run first. Once an operation is recorded,
  `JobOperator::execute` can return its durable replay outcome without any
  transformer implementation.
- Strict restart still calls `create_job_execution_with_definition`.
  Compatible restart calls the existing Gate-D
  `create_job_execution_with_upgrade` authority.
- Repository rejection remains authoritative for an unregistered, incompatible,
  stale, or otherwise invalid edge.

## Verification added

`crates/oxide-batch/tests/services.rs` adds:

- `compatible_restart_digest_binds_the_exact_durable_edge`: strict and
  compatible requests have distinct digests, and changes to edge key/revision,
  source/target identity, mappings, or state-transform descriptors change the
  compatible request digest.
- `compatible_restart_requires_exact_live_execution_and_replays_without_code`:
  missing and mismatched live executions leave both lifecycle and operator audit
  unchanged; the exact registered edge applies once; replay then succeeds
  through the ordinary `execute` path without live transform code.

Existing M4 service-contract tests remain unchanged. Existing Gate-D compatible
upgrade tests remain the authority for registry validation, atomic transformed
state materialization, rollback, concurrency, and fail-closed edge selection.

## PostgreSQL / crash disposition

#613 introduces no table, schema, counter, state envelope, or transaction
boundary. Compatible restart still uses the Gate-D atomic
`create_job_execution_with_upgrade` operation. Therefore a new #613-specific
PostgreSQL 15/18 process-kill boundary is reviewed **N/A**. The accepted
`postgres_compatible_upgrade_process_kill.rs` Gate-D evidence remains the
durability/crash authority, while normal PR PostgreSQL and conformance gates
must remain green for the candidate.

## Explicit non-goals

- no fork-as-operator audit integration;
- no explorer projection/query expansion;
- no registry/upgrade-edge redesign or compatibility inference;
- no transitive upgrade or `Force`;
- no CLI/API-server/hosted control-plane work;
- no M8-M14 behavior.

After #613, #199 still owns fork operator integration and bounded M7 explorer
projections/query forms. #200 owns final M7-wide conformance, documentation, and
exit acceptance.
