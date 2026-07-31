//! Retry, backoff, skip, and rollback policy contracts.
//!
//! These cases cover the runtime-neutral M3 fault-tolerance slice. Chunk
//! integration, durable reservation, and `PostgreSQL` state are exercised by the
//! dependent workstreams.

#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code)]
#[path = "support/random.rs"]
mod random;

use std::error::Error;
use std::time::Duration;

use oxide_batch::{
    BackoffKind, BackoffPolicy, ChunkDeliveryMode, ClassifierRevision, FailureCategory, FailureId,
    FailureSummary, FaultAction, FaultClassifier, FaultDecision, FaultDescriptor, FaultEvidence,
    FaultPhase, FaultPolicy, FaultPolicyError, FaultRule, RetryLimit, RetryOrdinal,
    RetryStateLimit, RollbackDisposition, SkipCounts, SkipLimit,
};
use random::SeededRandom;

const SEED: u64 = 0x4f78_6964_6533_4d33;

fn summary(category: FailureCategory) -> Result<FailureSummary, Box<dyn Error>> {
    Ok(FailureSummary::new(category, FailureId::new(7)?))
}

fn descriptor(
    phase: FaultPhase,
    category: FailureCategory,
    ordinal: u32,
    skips: SkipCounts,
) -> Result<FaultDescriptor, Box<dyn Error>> {
    Ok(FaultDescriptor::new(
        phase,
        summary(category)?,
        RetryOrdinal::new(ordinal)?,
        skips,
        true,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

fn revision() -> Result<ClassifierRevision, Box<dyn Error>> {
    Ok(ClassifierRevision::new("fault_policy_test_v1")?)
}

fn policy(
    rules: impl IntoIterator<Item = FaultRule>,
    retry_limit: u32,
    skip_limit: u64,
    backoff: BackoffPolicy,
) -> Result<FaultPolicy, Box<dyn Error>> {
    Ok(FaultPolicy::new(
        FaultClassifier::new(revision()?, rules)?,
        RetryLimit::new(retry_limit)?,
        RetryStateLimit::new(16)?,
        SkipLimit::new(skip_limit),
        backoff,
    )?)
}

/// Complete skip evidence for a located, rolled-back read failure.
fn read_skip_evidence() -> FaultEvidence {
    FaultEvidence::NONE
        .with_located(true)
        .with_known_rollback(true)
        .with_forward_checkpoint_proof(true)
}

#[test]
fn retry_limit_rejects_values_above_the_bounded_representation() {
    assert_eq!(RetryLimit::new(65_535).map(RetryLimit::get), Ok(65_535));
    assert_eq!(
        RetryLimit::new(65_536),
        Err(FaultPolicyError::RetryLimitOutOfRange { max: 65_535 })
    );
}

#[test]
fn retry_state_limit_requires_an_explicit_bound_within_capacity() {
    assert_eq!(
        RetryStateLimit::new(0),
        Err(FaultPolicyError::RetryStateLimitOutOfRange { min: 1, max: 256 })
    );
    assert_eq!(
        RetryStateLimit::new(257),
        Err(FaultPolicyError::RetryStateLimitOutOfRange { min: 1, max: 256 })
    );
    assert_eq!(RetryStateLimit::new(256).map(RetryStateLimit::get), Ok(256));
}

#[test]
fn retry_limit_permits_exactly_the_configured_reservations() -> Result<(), Box<dyn Error>> {
    let limit = RetryLimit::new(2)?;
    assert!(!limit.permits(RetryOrdinal::INITIAL));
    assert!(limit.permits(RetryOrdinal::new(1)?));
    assert!(limit.permits(RetryOrdinal::new(2)?));
    assert!(!limit.permits(RetryOrdinal::new(3)?));
    assert!(!RetryLimit::NONE.permits(RetryOrdinal::new(1)?));
    Ok(())
}

#[test]
fn skip_counts_keep_phases_distinct_and_reject_overflow() -> Result<(), Box<dyn Error>> {
    let counts = SkipCounts::ZERO
        .checked_increment(FaultPhase::Read)?
        .checked_increment(FaultPhase::Write)?
        .checked_increment(FaultPhase::Write)?;
    assert_eq!((counts.read(), counts.process(), counts.write()), (1, 0, 2));
    assert_eq!(counts.checked_total()?, 3);

    assert_eq!(
        SkipCounts::ZERO.checked_increment(FaultPhase::Transaction),
        Err(FaultPolicyError::PhaseNotSkippable {
            phase: FaultPhase::Transaction
        })
    );
    assert_eq!(
        SkipCounts::new(u64::MAX, 0, 0).checked_increment(FaultPhase::Read),
        Err(FaultPolicyError::SkipCountOverflow)
    );
    assert_eq!(
        SkipCounts::new(u64::MAX, 1, 0).checked_total(),
        Err(FaultPolicyError::SkipCountOverflow)
    );
    Ok(())
}

#[test]
fn classifier_rejects_ambiguous_and_fail_closed_rules() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        FaultRule::new(
            FaultPhase::Listener,
            FailureCategory::UserComponent,
            FaultAction::retry(),
        ),
        Err(FaultPolicyError::NotPolicyEligible {
            phase: FaultPhase::Listener,
            category: FailureCategory::UserComponent,
        })
    );
    assert_eq!(
        FaultRule::new(
            FaultPhase::Read,
            FailureCategory::UnknownCommit,
            FaultAction::retry(),
        ),
        Err(FaultPolicyError::NotPolicyEligible {
            phase: FaultPhase::Read,
            category: FailureCategory::UnknownCommit,
        })
    );
    assert_eq!(
        FaultRule::new(
            FaultPhase::Transaction,
            FailureCategory::Timeout,
            FaultAction::skip(RollbackDisposition::Rollback),
        ),
        Err(FaultPolicyError::PhaseNotSkippable {
            phase: FaultPhase::Transaction
        })
    );
    assert_eq!(
        FaultRule::new(
            FaultPhase::Write,
            FailureCategory::UserComponent,
            FaultAction::skip(RollbackDisposition::CommitSafeSkip),
        ),
        Err(FaultPolicyError::CommitSafeSkipPhase {
            phase: FaultPhase::Write
        })
    );

    let duplicate = FaultClassifier::new(
        revision()?,
        [
            FaultRule::new(
                FaultPhase::Read,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )?,
            FaultRule::new(
                FaultPhase::Read,
                FailureCategory::Timeout,
                FaultAction::fail(),
            )?,
        ],
    );
    assert_eq!(
        duplicate.err(),
        Some(FaultPolicyError::DuplicateRule {
            phase: FaultPhase::Read,
            category: FailureCategory::Timeout,
        })
    );
    Ok(())
}

#[test]
fn classifier_rules_are_bounded_by_the_eligible_phase_and_category_product()
-> Result<(), Box<dyn Error>> {
    let phases = [
        FaultPhase::Read,
        FaultPhase::Process,
        FaultPhase::Write,
        FaultPhase::Transaction,
        FaultPhase::Checkpoint,
        FaultPhase::Backoff,
    ];
    let categories = [
        FailureCategory::TransientInfrastructure,
        FailureCategory::PermanentInfrastructure,
        FailureCategory::UserComponent,
        FailureCategory::OptimisticConflict,
        FailureCategory::Timeout,
    ];
    let mut rules = Vec::new();
    for phase in phases {
        for category in categories {
            rules.push(FaultRule::new(phase, category, FaultAction::fail())?);
        }
    }
    let complete = FaultClassifier::new(revision()?, rules.clone())?;
    assert_eq!(complete.rules().len(), phases.len() * categories.len());

    rules.push(FaultRule::new(
        FaultPhase::Read,
        FailureCategory::Timeout,
        FaultAction::fail(),
    )?);
    assert_eq!(
        FaultClassifier::new(revision()?, rules).err(),
        Some(FaultPolicyError::DuplicateRule {
            phase: FaultPhase::Read,
            category: FailureCategory::Timeout,
        })
    );
    Ok(())
}

#[test]
fn policy_rejects_a_retry_rule_that_no_limit_can_satisfy() -> Result<(), Box<dyn Error>> {
    let classifier = FaultClassifier::new(
        revision()?,
        [FaultRule::new(
            FaultPhase::Process,
            FailureCategory::Timeout,
            FaultAction::retry(),
        )?],
    )?;
    let rejected = FaultPolicy::new(
        classifier,
        RetryLimit::NONE,
        RetryStateLimit::new(1)?,
        SkipLimit::NONE,
        BackoffPolicy::none(),
    );
    assert_eq!(
        rejected.err(),
        Some(FaultPolicyError::UnreachableRetryRule {
            phase: FaultPhase::Process,
            category: FailureCategory::Timeout,
        })
    );
    Ok(())
}

#[test]
fn unmatched_and_fail_closed_faults_roll_back() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Read,
            FailureCategory::Timeout,
            FaultAction::retry(),
        )?],
        1,
        0,
        BackoffPolicy::none(),
    )?;

    let unmatched = descriptor(
        FaultPhase::Read,
        FailureCategory::UserComponent,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&unmatched, FaultEvidence::NONE),
        FaultDecision::FailAndRollback
    );

    let listener = descriptor(
        FaultPhase::Listener,
        FailureCategory::UserComponent,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&listener, read_skip_evidence()),
        FaultDecision::FailAndRollback
    );

    let invariant = descriptor(
        FaultPhase::Read,
        FailureCategory::Invariant,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&invariant, read_skip_evidence()),
        FaultDecision::FailAndRollback
    );
    Ok(())
}

#[test]
fn unknown_commit_is_never_retried() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Read,
            FailureCategory::Timeout,
            FaultAction::retry(),
        )?],
        3,
        8,
        BackoffPolicy::none(),
    )?;
    let unknown = descriptor(
        FaultPhase::Transaction,
        FailureCategory::UnknownCommit,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&unknown, read_skip_evidence()),
        FaultDecision::Unknown
    );

    let cancelled = descriptor(
        FaultPhase::Read,
        FailureCategory::Cancelled,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&cancelled, FaultEvidence::NONE),
        FaultDecision::Stop
    );
    Ok(())
}

#[test]
fn retry_exhaustion_uses_initial_plus_reserved_retries() -> Result<(), Box<dyn Error>> {
    let backoff = BackoffPolicy::fixed(Duration::from_millis(250))?;
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Process,
            FailureCategory::TransientInfrastructure,
            FaultAction::retry(),
        )?],
        2,
        0,
        backoff,
    )?;

    for ordinal in 0..2_u32 {
        let fault = descriptor(
            FaultPhase::Process,
            FailureCategory::TransientInfrastructure,
            ordinal,
            SkipCounts::ZERO,
        )?;
        assert_eq!(
            policy.decide(&fault, FaultEvidence::NONE),
            FaultDecision::Retry {
                ordinal: RetryOrdinal::new(ordinal + 1)?,
                delay: Duration::from_millis(250),
            }
        );
    }

    let exhausted = descriptor(
        FaultPhase::Process,
        FailureCategory::TransientInfrastructure,
        2,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&exhausted, FaultEvidence::NONE),
        FaultDecision::FailAndRollback
    );
    Ok(())
}

#[test]
fn exhaustion_skips_only_when_the_same_rule_accepts_a_skip() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Read,
            FailureCategory::Timeout,
            FaultAction::retry_then_skip(RollbackDisposition::Rollback),
        )?],
        1,
        4,
        BackoffPolicy::none(),
    )?;

    let first = descriptor(
        FaultPhase::Read,
        FailureCategory::Timeout,
        0,
        SkipCounts::ZERO,
    )?;
    assert!(policy.decide(&first, read_skip_evidence()).is_retry());

    let exhausted = descriptor(
        FaultPhase::Read,
        FailureCategory::Timeout,
        1,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&exhausted, read_skip_evidence()),
        FaultDecision::Skip {
            disposition: RollbackDisposition::Rollback
        }
    );
    Ok(())
}

#[test]
fn skip_limit_is_shared_across_phases() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [
            FaultRule::new(
                FaultPhase::Read,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::Rollback),
            )?,
            FaultRule::new(
                FaultPhase::Write,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::Rollback),
            )?,
        ],
        0,
        2,
        BackoffPolicy::none(),
    )?;

    let inherited = SkipCounts::new(1, 0, 0);
    let write = descriptor(
        FaultPhase::Write,
        FailureCategory::UserComponent,
        0,
        inherited,
    )?;
    assert_eq!(
        policy.decide(&write, read_skip_evidence()),
        FaultDecision::Skip {
            disposition: RollbackDisposition::Rollback
        }
    );

    let at_limit = descriptor(
        FaultPhase::Read,
        FailureCategory::UserComponent,
        0,
        SkipCounts::new(1, 0, 1),
    )?;
    assert_eq!(
        policy.decide(&at_limit, read_skip_evidence()),
        FaultDecision::FailAndRollback
    );
    Ok(())
}

#[test]
fn write_skip_requires_located_known_rollback() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Write,
            FailureCategory::UserComponent,
            FaultAction::skip(RollbackDisposition::Rollback),
        )?],
        0,
        8,
        BackoffPolicy::none(),
    )?;
    let fault = descriptor(
        FaultPhase::Write,
        FailureCategory::UserComponent,
        0,
        SkipCounts::ZERO,
    )?;

    assert_eq!(
        policy.decide(&fault, FaultEvidence::NONE.with_known_rollback(true)),
        FaultDecision::FailAndRollback,
        "an unlocated write cannot be skipped"
    );
    assert_eq!(
        policy.decide(&fault, FaultEvidence::NONE.with_located(true)),
        FaultDecision::FailAndRollback,
        "an ambiguous write effect cannot be skipped"
    );
    assert_eq!(
        policy.decide(&fault, FaultEvidence::new(true, true, false)),
        FaultDecision::Skip {
            disposition: RollbackDisposition::Rollback
        }
    );
    Ok(())
}

#[test]
fn read_skip_requires_forward_checkpoint_progress() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Read,
            FailureCategory::UserComponent,
            FaultAction::skip(RollbackDisposition::Rollback),
        )?],
        0,
        8,
        BackoffPolicy::none(),
    )?;
    let fault = descriptor(
        FaultPhase::Read,
        FailureCategory::UserComponent,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&fault, FaultEvidence::NONE.with_located(true)),
        FaultDecision::FailAndRollback
    );
    assert_eq!(
        policy.decide(&fault, read_skip_evidence()),
        FaultDecision::Skip {
            disposition: RollbackDisposition::Rollback
        }
    );
    Ok(())
}

#[test]
fn commit_safe_skip_requires_capability_and_evidence() -> Result<(), Box<dyn Error>> {
    let policy = policy(
        [FaultRule::new(
            FaultPhase::Process,
            FailureCategory::UserComponent,
            FaultAction::skip(RollbackDisposition::CommitSafeSkip),
        )?],
        0,
        8,
        BackoffPolicy::none(),
    )?;
    assert!(policy.requires_commit_safe_skip());
    assert_eq!(
        policy.validate_capabilities(false),
        Err(FaultPolicyError::CommitSafeSkipUnsupported)
    );
    assert_eq!(policy.validate_capabilities(true), Ok(()));

    let fault = descriptor(
        FaultPhase::Process,
        FailureCategory::UserComponent,
        0,
        SkipCounts::ZERO,
    )?;
    assert_eq!(
        policy.decide(&fault, FaultEvidence::new(true, false, true)),
        FaultDecision::FailAndRollback,
        "a possible external effect cannot commit safely"
    );
    assert_eq!(
        policy.decide(&fault, read_skip_evidence()),
        FaultDecision::Skip {
            disposition: RollbackDisposition::CommitSafeSkip
        }
    );
    Ok(())
}

#[test]
fn backoff_arithmetic_is_capped() -> Result<(), Box<dyn Error>> {
    let backoff =
        BackoffPolicy::exponential(Duration::from_millis(100), 3, Duration::from_secs(10))?;
    assert_eq!(backoff.kind(), BackoffKind::Exponential);
    assert_eq!(backoff.delay_for(RetryOrdinal::INITIAL), Duration::ZERO);
    assert_eq!(
        backoff.delay_for(RetryOrdinal::new(1)?),
        Duration::from_millis(100)
    );
    assert_eq!(
        backoff.delay_for(RetryOrdinal::new(2)?),
        Duration::from_millis(300)
    );
    assert_eq!(
        backoff.delay_for(RetryOrdinal::new(3)?),
        Duration::from_millis(900)
    );
    assert_eq!(
        backoff.delay_for(RetryOrdinal::new(5)?),
        Duration::from_millis(8_100)
    );
    for ordinal in [6_u32, 7, 32, 1_024, 65_535] {
        assert_eq!(
            backoff.delay_for(RetryOrdinal::new(ordinal)?),
            Duration::from_secs(10),
            "ordinal {ordinal} must saturate at the configured maximum"
        );
    }
    Ok(())
}

#[test]
fn backoff_rejects_unbounded_or_degenerate_schedules() -> Result<(), Box<dyn Error>> {
    let day = Duration::from_hours(24);
    assert_eq!(
        BackoffPolicy::fixed(day + Duration::from_nanos(1)).err(),
        Some(FaultPolicyError::BackoffDelayTooLong {
            max_seconds: 86_400
        })
    );
    assert_eq!(
        BackoffPolicy::exponential(Duration::from_secs(1), 0, Duration::from_secs(2)).err(),
        Some(FaultPolicyError::ZeroBackoffMultiplier)
    );
    assert_eq!(
        BackoffPolicy::exponential(Duration::from_secs(4), 2, Duration::from_secs(2)).err(),
        Some(FaultPolicyError::BackoffMaximumBelowInitial)
    );
    assert_eq!(
        BackoffPolicy::fixed(day)?.delay_for(RetryOrdinal::new(9)?),
        day
    );
    assert_eq!(
        BackoffPolicy::none().delay_for(RetryOrdinal::new(9)?),
        Duration::ZERO
    );
    Ok(())
}

#[test]
fn backoff_schedules_are_monotonic_and_bounded_for_random_ordinals() -> Result<(), Box<dyn Error>> {
    let mut random = SeededRandom::new(SEED);
    let maximum = Duration::from_secs(30);
    let backoff = BackoffPolicy::exponential(Duration::from_millis(5), 2, maximum)?;
    for case in 0..256_u32 {
        let low = u32::try_from(random.next_u64() % 4_096).unwrap_or(0);
        let high = low.saturating_add(u32::try_from(random.next_u64() % 4_096).unwrap_or(0));
        let low_delay = backoff.delay_for(RetryOrdinal::new(low)?);
        let high_delay = backoff.delay_for(RetryOrdinal::new(high)?);
        assert!(
            low_delay <= high_delay,
            "seed {SEED:#x} case {case}: {low} -> {low_delay:?} exceeded {high} -> {high_delay:?}"
        );
        assert!(
            high_delay <= maximum,
            "seed {SEED:#x} case {case}: ordinal {high} exceeded the configured maximum"
        );
    }
    Ok(())
}

#[test]
fn descriptor_exposes_only_framework_owned_classification_input() -> Result<(), Box<dyn Error>> {
    let fault = descriptor(
        FaultPhase::Write,
        FailureCategory::OptimisticConflict,
        2,
        SkipCounts::new(1, 2, 3),
    )?;
    assert_eq!(fault.phase(), FaultPhase::Write);
    assert_eq!(fault.category(), FailureCategory::OptimisticConflict);
    assert_eq!(fault.retry_ordinal().get(), 2);
    assert_eq!(fault.committed_skips().checked_total()?, 6);
    assert!(fault.is_transaction_open());
    assert_eq!(fault.delivery_mode(), ChunkDeliveryMode::AtomicSameResource);
    assert_eq!(fault.failure_id(), fault.summary().failure_id());

    let rendered = format!("{fault:?}");
    for forbidden in ["Sql", "sqlx", "tokio", "serde"] {
        assert!(
            !rendered.contains(forbidden),
            "descriptor diagnostics leaked {forbidden}"
        );
    }
    Ok(())
}

#[test]
fn policy_eligibility_fails_closed_for_framework_categories() {
    for category in [
        FailureCategory::InvalidDefinition,
        FailureCategory::DuplicateExecution,
        FailureCategory::IllegalTransition,
        FailureCategory::Cancelled,
        FailureCategory::Serialization,
        FailureCategory::Invariant,
        FailureCategory::UnsupportedCapability,
        FailureCategory::UnknownCommit,
    ] {
        assert!(
            !category.is_policy_eligible(),
            "{category:?} must never be retried or skipped in M3"
        );
    }
    for category in [
        FailureCategory::TransientInfrastructure,
        FailureCategory::PermanentInfrastructure,
        FailureCategory::UserComponent,
        FailureCategory::OptimisticConflict,
        FailureCategory::Timeout,
    ] {
        assert!(category.is_policy_eligible());
    }
    assert!(!FaultPhase::Listener.is_policy_eligible());
    assert!(!FaultPhase::Transaction.is_skippable());
    assert!(!FaultPhase::Write.allows_commit_safe_skip());
}
