# Domain Glossary

**State:** Proposed

| Term | OxideBatch meaning |
| --- | --- |
| Job | Named executable batch definition composed of steps and flow |
| Job name | Stable logical name used with identifying parameters for instance identity |
| Job definition version | Compatibility identity of executable job structure; final encoding is undecided |
| Job parameters | Typed launch values; identifying values participate in instance identity |
| Job instance | Logical occurrence of a named job for canonical identifying parameters |
| Job execution | One launch/restart attempt for a job instance |
| Step | Independent phase in a job definition |
| Step execution | One attempt to execute a step |
| Tasklet | Step body that performs one repeatable unit until completion policy ends |
| Chunk | Bounded set of items processed under one completion/commit boundary |
| Item reader | Component that obtains the next input and owns documented restart position |
| Item processor | Optional transformation/filter component |
| Item writer | Component that applies a collection of outputs |
| Execution context | Bounded, versioned durable state used to restart a job or step |
| Checkpoint | Durable restart position plus associated context/counters at a commit boundary |
| Batch status | Framework lifecycle state |
| Exit status | Flow/operator-facing outcome separate from lifecycle |
| Restart | New execution attempts for an existing non-terminal/restartable job instance |
| Retry | Re-attempt of a failed operation under a bounded policy |
| Skip | Intentional classification of an item as not successfully processed |
| Rollback | Reversal of a transactional attempt before checkpoint advancement |
| Abandon | Operator/framework decision that an execution is terminal and not restartable |
| Recover | Explicit resolution of an ambiguous/orphaned execution into an actionable state |
| Repository | Authority for instance/execution identity, lifecycle, context, and concurrency |
| Listener | Ordered callback observing or influencing defined lifecycle points |
| Decider | Component selecting a flow transition from durable execution information |
| Partition | Explicit subset of work with identity, ownership, and aggregate status |
| Delivery guarantee | Scope-specific promise such as atomic transactional commit or at-least-once |

Differences from Spring Batch are recorded in the compatibility matrix rather
than hidden behind a familiar term.
