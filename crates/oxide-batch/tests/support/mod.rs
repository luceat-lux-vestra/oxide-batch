//! Shared, deterministic integration-test support.

mod backoff;
mod clock;
mod events;
mod fixtures;
mod ids;
mod random;
mod scenario;
mod secrets;
mod timeout;

pub use backoff::ControlledBackoff;
pub use clock::{ManualClock, ManualClockError};
pub use events::{CapturedEvent, EventCapture};
pub use fixtures::{FixtureProvenance, FixtureProvenanceError};
pub use ids::{DeterministicIds, IdSequenceError};
pub use random::SeededRandom;
pub use scenario::{
    DiagnosticContext, ScenarioId, ScenarioIdError, ScenarioReport, ScenarioStatus,
};
pub use secrets::{SENTINEL_SECRET, assert_sentinel_absent};
pub use timeout::{BoundedTimeout, BoundedTimeoutError};
