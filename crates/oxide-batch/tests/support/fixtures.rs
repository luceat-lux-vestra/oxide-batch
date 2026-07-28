use std::error::Error;
use std::fmt;

use super::ScenarioId;

/// Reviewable origin and regeneration metadata for a test fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureProvenance {
    /// Compatibility scenario that owns the fixture directory.
    pub scenario_id: ScenarioId,
    /// Exact external reference or `independently-authored synthetic fixture`.
    pub source: String,
    /// Version of the fixture's serialized format.
    pub format_version: String,
    /// Command or procedure that regenerates generated data.
    pub regeneration: String,
    /// Seed for generated data, when randomness was used.
    pub seed: Option<u64>,
    /// Confirms that the data contains no production extract.
    pub synthetic: bool,
}

impl FixtureProvenance {
    /// Validates the fields required by the accepted fixture policy.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureProvenanceError`] when provenance is incomplete or the
    /// fixture is not declared synthetic.
    pub fn validate(&self) -> Result<(), FixtureProvenanceError> {
        if !self.synthetic {
            return Err(FixtureProvenanceError::NotSynthetic);
        }
        if self.source.trim().is_empty() {
            return Err(FixtureProvenanceError::MissingSource);
        }
        if self.format_version.trim().is_empty() {
            return Err(FixtureProvenanceError::MissingFormatVersion);
        }
        if self.regeneration.trim().is_empty() {
            return Err(FixtureProvenanceError::MissingRegeneration);
        }
        Ok(())
    }
}

/// Invalid fixture provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureProvenanceError {
    /// A fixture was not confirmed to be synthetic.
    NotSynthetic,
    /// No source or independent-authorship statement was supplied.
    MissingSource,
    /// No serialized format version was supplied.
    MissingFormatVersion,
    /// No regeneration command or procedure was supplied.
    MissingRegeneration,
}

impl fmt::Display for FixtureProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotSynthetic => "fixture must be synthetic",
            Self::MissingSource => "fixture provenance requires a source",
            Self::MissingFormatVersion => "fixture provenance requires a format version",
            Self::MissingRegeneration => "fixture provenance requires regeneration instructions",
        })
    }
}

impl Error for FixtureProvenanceError {}
