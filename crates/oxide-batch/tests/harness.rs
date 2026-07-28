//! Focused tests for deterministic support and shared semantic contracts.

mod conformance;
mod contract;
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::mpsc;
use std::time::{Duration, UNIX_EPOCH};

use contract::{CreateInstanceOutcome, RepositoryContract, run_repository_contract};
use oxide_batch::{JobInstanceId, JobInstanceKey};
use support::{
    BoundedTimeout, BoundedTimeoutError, ControlledBackoff, DeterministicIds, DiagnosticContext,
    EventCapture, FixtureProvenance, FixtureProvenanceError, IdSequenceError, ManualClock,
    ManualClockError, SENTINEL_SECRET, ScenarioId, ScenarioIdError, ScenarioReport, ScenarioStatus,
    SeededRandom, assert_sentinel_absent,
};

#[test]
fn manual_clock_and_id_sequence_advance_without_time_or_uuid_sources() -> Result<(), Box<dyn Error>>
{
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(10));
    let clone = clock.clone();
    assert_eq!(
        clone.advance(Duration::from_secs(5))?,
        UNIX_EPOCH + Duration::from_secs(15)
    );
    assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_secs(15));
    clock.set(UNIX_EPOCH + Duration::from_secs(20));
    assert_eq!(clone.now(), UNIX_EPOCH + Duration::from_secs(20));

    let ids = DeterministicIds::new(NonZeroU64::new(40).ok_or(IdSequenceError::Exhausted)?);
    assert_eq!(ids.next_job_instance()?.get(), 40);
    assert_eq!(ids.next_job_execution()?.get(), 41);
    assert_eq!(ids.next_step_execution()?.get(), 42);
    assert_eq!(ids.next_failure()?.get(), 43);

    let exhausted = DeterministicIds::new(NonZeroU64::MAX);
    assert_eq!(exhausted.next_raw()?, u64::MAX);
    assert_eq!(exhausted.next_raw(), Err(IdSequenceError::Exhausted));
    assert_eq!(
        ManualClockError::Overflow.to_string(),
        "manual clock advance overflowed"
    );
    Ok(())
}

#[test]
fn seeded_random_and_backoff_are_exactly_reproducible() {
    let mut first = SeededRandom::new(0x5eed);
    let mut second = SeededRandom::new(0x5eed);
    let first_values = (0..4).map(|_| first.next_u64()).collect::<Vec<_>>();
    let second_values = (0..4).map(|_| second.next_u64()).collect::<Vec<_>>();
    assert_eq!(first.seed(), 0x5eed);
    assert_eq!(first_values, second_values);
    assert_eq!(first.index(0), None);
    assert_eq!(second.index(0), None);

    let expected = [
        Duration::from_millis(10),
        Duration::from_millis(25),
        Duration::from_millis(50),
    ];
    let mut backoff = ControlledBackoff::new(expected);
    assert_eq!(backoff.next_delay(), Some(expected[0]));
    assert_eq!(backoff.next_delay(), Some(expected[1]));
    assert_eq!(backoff.requested(), 2);
    backoff.reset();
    assert_eq!(backoff.next_delay(), Some(expected[0]));
    assert_eq!(backoff.requested(), 1);
}

#[test]
fn event_capture_preserves_order_and_bounded_receive_never_waits_forever()
-> Result<(), Box<dyn Error>> {
    let events = EventCapture::new();
    assert_eq!(events.record("execution.created", "id=41"), 0);
    assert_eq!(events.record("execution.started", "id=41"), 1);
    let snapshot = events.snapshot();
    assert_eq!(snapshot[0].sequence(), 0);
    assert_eq!(snapshot[0].name(), "execution.created");
    assert_eq!(snapshot[1].detail(), "id=41");
    events.clear();
    assert!(events.snapshot().is_empty());

    let timeout = BoundedTimeout::new(Duration::from_millis(10))?;
    assert_eq!(timeout.duration(), Duration::from_millis(10));
    let (sender, receiver) = mpsc::channel();
    sender.send("ready")?;
    assert_eq!(timeout.receive(&receiver)?, "ready");
    assert_eq!(
        BoundedTimeout::new(Duration::ZERO),
        Err(BoundedTimeoutError::Zero)
    );
    assert!(matches!(
        BoundedTimeout::new(Duration::from_secs(31)),
        Err(BoundedTimeoutError::ExceedsMaximum { .. })
    ));
    Ok(())
}

#[test]
fn matrix_rows_are_valid_unique_and_reported_with_executable_names() -> Result<(), Box<dyn Error>> {
    let mut identifiers = BTreeSet::new();
    let mut names = BTreeSet::new();
    for (identifier, name) in conformance::MATRIX_SCENARIOS {
        let scenario_id = conformance::scenario_id(identifier)?;
        assert!(identifiers.insert(scenario_id.clone()));
        assert!(names.insert(*name));
        let report =
            ScenarioReport::new(scenario_id, *name, ScenarioStatus::Passed, 17, Vec::new());
        assert_eq!(report.scenario_id().as_str(), *identifier);
        assert_eq!(report.scenario_name(), *name);
        assert_eq!(report.status(), ScenarioStatus::Passed);
        assert_eq!(report.seed(), 17);
        assert!(report.events().is_empty());
    }
    assert_eq!(ScenarioId::new("job-instance-001"), Err(ScenarioIdError));
    assert_eq!(
        ScenarioStatus::Failed,
        ScenarioReport::new(
            ScenarioId::new("TEST-001")?,
            "test_failure",
            ScenarioStatus::Failed,
            1,
            Vec::new(),
        )
        .status()
    );
    Ok(())
}

#[test]
fn fixture_provenance_and_sentinel_policy_are_enforced() -> Result<(), Box<dyn Error>> {
    let provenance = FixtureProvenance {
        scenario_id: ScenarioId::new("VS-LAUNCH-001")?,
        source: String::from("independently-authored synthetic fixture"),
        format_version: String::from("text-v1"),
        regeneration: String::from("review PROVENANCE.md; no generator required"),
        seed: None,
        synthetic: true,
    };
    provenance.validate()?;
    assert_eq!(provenance.scenario_id.as_str(), "VS-LAUNCH-001");
    assert_eq!(provenance.seed, None);

    let mut invalid = provenance.clone();
    invalid.synthetic = false;
    assert_eq!(
        invalid.validate(),
        Err(FixtureProvenanceError::NotSynthetic)
    );
    invalid.synthetic = true;
    invalid.source.clear();
    assert_eq!(
        invalid.validate(),
        Err(FixtureProvenanceError::MissingSource)
    );

    let safe_log = "parameter=<redacted>";
    let safe_event = "field_count=1";
    assert_sentinel_absent([("log", safe_log), ("event", safe_event)]);
    assert!(!safe_log.contains(SENTINEL_SECRET));
    Ok(())
}

#[test]
fn shared_repository_contract_runs_against_a_test_adapter() -> Result<(), Box<dyn Error>> {
    run_repository_contract(|| Ok(MemoryContractRepository::default()))?;
    Ok(())
}

/// A passing CI test containing an intentionally failed assertion.
///
/// `should_panic` keeps the suite green while proving that the exact scenario,
/// seed, and ordered events are present when a generated case fails.
#[test]
#[should_panic(expected = "scenario=JOB-INSTANCE-001 seed=23 events=[0:generated(candidate=7)]")]
fn failing_example_reports_reproducible_seed_and_events() {
    let events = EventCapture::new();
    events.record("generated", "candidate=7");
    let Ok(scenario_id) = ScenarioId::new("JOB-INSTANCE-001") else {
        return;
    };
    let diagnostics = DiagnosticContext::new(scenario_id, 23, events.snapshot());
    assert_eq!(7, 8, "{diagnostics}");
}

#[derive(Debug, Default)]
struct MemoryContractRepository {
    instances: BTreeMap<JobInstanceKey, JobInstanceId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MemoryContractError;

impl fmt::Display for MemoryContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("in-memory contract adapter failed")
    }
}

impl Error for MemoryContractError {}

impl RepositoryContract for MemoryContractRepository {
    type Error = MemoryContractError;

    fn backend_name(&self) -> &'static str {
        "test-memory-adapter"
    }

    fn create_instance(
        &mut self,
        key: &JobInstanceKey,
        proposed_id: JobInstanceId,
    ) -> Result<CreateInstanceOutcome, Self::Error> {
        if let Some(existing) = self.instances.get(key) {
            return Ok(CreateInstanceOutcome::Existing(*existing));
        }
        self.instances.insert(key.clone(), proposed_id);
        Ok(CreateInstanceOutcome::Created(proposed_id))
    }

    fn find_instance(&self, key: &JobInstanceKey) -> Result<Option<JobInstanceId>, Self::Error> {
        Ok(self.instances.get(key).copied())
    }
}
