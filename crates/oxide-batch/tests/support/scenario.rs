use std::error::Error;
use std::fmt;

use super::CapturedEvent;

/// A validated compatibility-matrix row identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScenarioId(String);

impl ScenarioId {
    /// Validates an uppercase ASCII identifier such as `JOB-EXEC-001`.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioIdError`] when the identifier cannot be matched
    /// reliably to a compatibility-matrix row.
    pub fn new(value: impl Into<String>) -> Result<Self, ScenarioIdError> {
        let value = value.into();
        let valid = !value.is_empty()
            && !value.starts_with('-')
            && !value.ends_with('-')
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-');
        if !valid {
            return Err(ScenarioIdError);
        }
        Ok(Self(value))
    }

    /// Returns the stable matrix identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ScenarioId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Invalid compatibility-matrix scenario identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScenarioIdError;

impl fmt::Display for ScenarioIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scenario ID must contain uppercase ASCII letters, digits, and hyphens")
    }
}

impl Error for ScenarioIdError {}

/// Test evidence state for a conformance scenario.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioStatus {
    /// The scenario matched its expected observations.
    Passed,
    /// The scenario produced a different observation.
    Failed,
}

/// Reproduction details included with a test assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticContext {
    scenario_id: ScenarioId,
    seed: u64,
    events: Vec<CapturedEvent>,
}

impl DiagnosticContext {
    /// Creates diagnostics from stable inputs.
    #[must_use]
    pub fn new(scenario_id: ScenarioId, seed: u64, events: Vec<CapturedEvent>) -> Self {
        Self {
            scenario_id,
            seed,
            events,
        }
    }
}

impl fmt::Display for DiagnosticContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "scenario={} seed={} events=[",
            self.scenario_id, self.seed
        )?;
        for (index, event) in self.events.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(
                formatter,
                "{}:{}({})",
                event.sequence(),
                event.name(),
                event.detail()
            )?;
        }
        formatter.write_str("]")
    }
}

/// A machine-readable conformance result with stable reproduction metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioReport {
    scenario_id: ScenarioId,
    scenario_name: String,
    status: ScenarioStatus,
    seed: u64,
    events: Vec<CapturedEvent>,
}

impl ScenarioReport {
    /// Builds a scenario report.
    #[must_use]
    pub fn new(
        scenario_id: ScenarioId,
        scenario_name: impl Into<String>,
        status: ScenarioStatus,
        seed: u64,
        events: Vec<CapturedEvent>,
    ) -> Self {
        Self {
            scenario_id,
            scenario_name: scenario_name.into(),
            status,
            seed,
            events,
        }
    }

    /// Returns the compatibility-matrix identifier.
    #[must_use]
    pub fn scenario_id(&self) -> &ScenarioId {
        &self.scenario_id
    }

    /// Returns the executable scenario name.
    #[must_use]
    pub fn scenario_name(&self) -> &str {
        &self.scenario_name
    }

    /// Returns the evidence result.
    #[must_use]
    pub const fn status(&self) -> ScenarioStatus {
        self.status
    }

    /// Returns the reproduction seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the normalized observations.
    #[must_use]
    pub fn events(&self) -> &[CapturedEvent] {
        &self.events
    }
}
