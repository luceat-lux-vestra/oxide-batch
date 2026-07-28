# Spring Batch Compatibility Contract

**State:** Accepted

## Reference baseline

The proposed normative reference is the **Spring Batch 6.0 conceptual and
behavioral model**. Patch releases are evidence for clarification and bug
behavior, not an automatic expansion of OxideBatch scope. Each supported
behavior must be listed in a versioned compatibility matrix and exercised by a
conformance scenario.

Spring Batch documentation defines the domain language, job and step
configuration, restartability, metadata, scaling, retry, testing, and
observability. OxideBatch uses those concepts as comparison points while
retaining Rust-native APIs.

References:

- [Spring Batch reference overview](https://docs.spring.io/spring-batch/reference/)
- [Configuring and running a job](https://docs.spring.io/spring-batch/reference/job/configuring-job.html)
- [Configuring a step](https://docs.spring.io/spring-batch/reference/step.html)
- [Advanced metadata usage](https://docs.spring.io/spring-batch/reference/job/advanced-meta-data.html)

## Compatibility levels

Every feature is classified independently:

| Level | Promise |
| --- | --- |
| Semantic | Same documented domain meaning and lifecycle outcome |
| Behavioral | Equivalent observable result for named conformance scenarios |
| Operational | Equivalent operator capability, possibly through a different API |
| Data import | Explicit one-way conversion is supported and versioned |
| Schema | Both frameworks can safely use the same metadata schema |
| API/source | Java configuration or APIs can be used unchanged |

The 1.0 target is semantic and selected behavioral/operational compatibility.
Data import is deferred. Schema and API/source compatibility are explicit
non-goals.

## Initial behavior target

- `Job` contains one or more `Step` definitions.
- identifying parameters determine `JobInstance` identity.
- each launch attempt creates a distinct `JobExecution`.
- each step attempt creates a distinct `StepExecution`.
- batch lifecycle status is separate from user-facing exit status.
- an execution context stores restart data at documented commit boundaries.
- completed job instances cannot be launched again with the same identifying
  parameters.
- failed or stopped executions may restart when the job is restartable.
- abandoned executions are not restartable.
- unknown or orphaned running states require an explicit recovery decision.
- chunk processing repeats read/process/write and commits at the configured
  completion boundary.

These statements are requirements, not yet claims about released behavior.

## State-model work required in M0

The lifecycle specification must define:

- allowed and forbidden transitions among starting, started, stopping, stopped,
  failed, completed, abandoned, and unknown;
- how listener failures affect status and exit status;
- which transition and checkpoint writes share a transaction;
- optimistic-lock conflict behavior;
- restart selection when multiple executions exist;
- clock, cancellation, and process-crash behavior;
- counter and execution-context persistence rules.

## Conformance evidence

The compatibility matrix will contain:

- behavior identifier and source reference;
- OxideBatch support level and first supported version;
- executable scenario name;
- known differences and rationale;
- metadata and telemetry observations where relevant.

Documentation language must say “inspired by” until the corresponding
behavioral matrix has executable evidence.
