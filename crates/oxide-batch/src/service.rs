//! Bounded operator, explorer, and retention services.
//!
//! The M4 services are portable and runtime neutral. They validate a bounded
//! request envelope, enforce lifecycle, version, definition, idempotency, and
//! query bounds, and record append-only audit rows. They never authenticate a
//! caller, never accept a credential, and never treat the supplied actor
//! reference as proof of authorization. A deployment authorizes the action's
//! [`AuthorizationClass`](crate::AuthorizationClass) before invoking a service.
//!
//! The request envelope, the durable records the services exchange with a
//! repository, and the ports they call live in `oxide-batch-repository`.

mod explorer;
mod operator;
mod recovery;
mod retention;

pub use explorer::JobExplorer;
pub use operator::{JobOperator, OperatorError, OperatorOutcome};
pub use recovery::RecoveryProposer;
pub use retention::{RetentionReport, RetentionService};
