//! Durable fault-state format, checksum, and bounded reservation contracts.
//!
//! These scenarios own the runtime-neutral half of `META-UPGRADE-001`:
//! the canonical bytes, the published empty-state checksum that migration
//! `0002` installs, and the corruption classes that must fail closed before any
//! component work begins.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::fmt::Write as _;

use oxide_batch::{
    ClassifierRevision, FailureCategory, FaultPhase, FaultStateEntry, FaultStateEnvelope,
    FaultStateError, FaultStateFormatError, RetryKey, RetryLimit, RetryOrdinal, RetryStateLimit,
};

/// The checksum migration `0002` installs on every schema-1 step execution.
const EMPTY_STATE_CHECKSUM: &str =
    "a491114819e0d3bd8b7ca004dc0636f95b45e2fcb1a67ddb5726beaea12f9922";

fn key(seed: u8) -> RetryKey {
    RetryKey::from_bytes([seed; 32])
}

fn revision() -> ClassifierRevision {
    ClassifierRevision::new("import_v1").expect("revision fixture must be valid")
}

fn entry(seed: u8, ordinal: u32) -> FaultStateEntry {
    FaultStateEntry::new(
        key(seed),
        FaultPhase::Write,
        FailureCategory::Timeout,
        RetryOrdinal::new(ordinal).expect("ordinal fixture must be valid"),
        revision(),
    )
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

fn state_limit(value: u32) -> RetryStateLimit {
    RetryStateLimit::new(value).expect("state limit fixture must be valid")
}

fn retry_limit(value: u32) -> RetryLimit {
    RetryLimit::new(value).expect("retry limit fixture must be valid")
}

fn reload(envelope: &FaultStateEnvelope) -> Result<FaultStateEnvelope, FaultStateFormatError> {
    FaultStateEnvelope::from_canonical_json(
        FaultStateEnvelope::FORMAT_VERSION,
        FaultStateEnvelope::FORMAT,
        FaultStateEnvelope::SCHEMA_VERSION,
        &envelope.to_canonical_json()?,
        &envelope.checksum()?,
    )
}

#[test]
fn empty_state_matches_the_published_migration_vector() -> Result<(), Box<dyn Error>> {
    let empty = FaultStateEnvelope::empty();
    assert!(empty.is_empty());
    assert_eq!(empty.checkpoint_digest(), &[0_u8; 32]);
    assert_eq!(
        String::from_utf8(empty.to_canonical_json()?)?,
        format!("{{\"checkpoint\":\"{}\",\"entries\":[]}}", "0".repeat(64))
    );
    assert_eq!(hex(&empty.checksum()?), EMPTY_STATE_CHECKSUM);
    Ok(())
}

#[test]
fn entries_round_trip_in_digest_order() -> Result<(), Box<dyn Error>> {
    let envelope = FaultStateEnvelope::new([9; 32], [entry(3, 2), entry(1, 1)])?;
    let digests: Vec<_> = envelope
        .entries()
        .iter()
        .map(|retained| *retained.key().as_bytes())
        .collect();
    assert_eq!(digests, vec![[1; 32], [3; 32]]);

    let restored = reload(&envelope)?;
    assert_eq!(restored, envelope);
    assert_eq!(
        restored.reserved_ordinal(key(3)),
        Some(RetryOrdinal::new(2)?)
    );
    assert_eq!(restored.reserved_ordinal(key(7)), None);
    Ok(())
}

#[test]
fn duplicate_and_unsorted_state_is_corruption() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        FaultStateEnvelope::new([9; 32], [entry(1, 1), entry(1, 2)]).err(),
        Some(FaultStateFormatError::DuplicateKey)
    );

    let sorted = FaultStateEnvelope::new([9; 32], [entry(1, 1), entry(3, 1)])?;
    let payload = String::from_utf8(sorted.to_canonical_json()?)?;
    let reversed = payload
        .replace(&hex(&[1; 32]), "SWAP")
        .replace(&hex(&[3; 32]), &hex(&[1; 32]));
    let reversed = reversed.replace("SWAP", &hex(&[3; 32]));
    assert_eq!(
        FaultStateEnvelope::from_canonical_json(
            FaultStateEnvelope::FORMAT_VERSION,
            FaultStateEnvelope::FORMAT,
            FaultStateEnvelope::SCHEMA_VERSION,
            reversed.as_bytes(),
            &sorted.checksum()?,
        )
        .err(),
        Some(FaultStateFormatError::UnsortedEntries)
    );
    Ok(())
}

#[test]
fn tampered_state_fails_closed() -> Result<(), Box<dyn Error>> {
    let envelope = FaultStateEnvelope::new([9; 32], [entry(1, 1)])?;
    let bytes = envelope.to_canonical_json()?;
    let mut checksum = envelope.checksum()?;
    checksum[0] ^= 0xff;
    assert_eq!(
        FaultStateEnvelope::from_canonical_json(
            FaultStateEnvelope::FORMAT_VERSION,
            FaultStateEnvelope::FORMAT,
            FaultStateEnvelope::SCHEMA_VERSION,
            &bytes,
            &checksum,
        )
        .err(),
        Some(FaultStateFormatError::ChecksumMismatch)
    );

    assert_eq!(
        FaultStateEnvelope::from_canonical_json(
            FaultStateEnvelope::FORMAT_VERSION,
            FaultStateEnvelope::FORMAT,
            FaultStateEnvelope::SCHEMA_VERSION + 1,
            &bytes,
            &envelope.checksum()?,
        )
        .err(),
        Some(FaultStateFormatError::UnsupportedSchemaVersion)
    );
    assert_eq!(
        FaultStateEnvelope::from_canonical_json(
            FaultStateEnvelope::FORMAT_VERSION + 1,
            FaultStateEnvelope::FORMAT,
            FaultStateEnvelope::SCHEMA_VERSION,
            &bytes,
            &envelope.checksum()?,
        )
        .err(),
        Some(FaultStateFormatError::UnsupportedFormat)
    );
    assert_eq!(
        FaultStateEnvelope::from_canonical_json(
            FaultStateEnvelope::FORMAT_VERSION,
            "other.format",
            FaultStateEnvelope::SCHEMA_VERSION,
            &bytes,
            &envelope.checksum()?,
        )
        .err(),
        Some(FaultStateFormatError::UnsupportedFormat)
    );

    let unknown = String::from_utf8(bytes)?.replace("\"write\"", "\"teleport\"");
    let unknown = FaultStateEnvelope::from_canonical_json(
        FaultStateEnvelope::FORMAT_VERSION,
        FaultStateEnvelope::FORMAT,
        FaultStateEnvelope::SCHEMA_VERSION,
        unknown.as_bytes(),
        &envelope.checksum()?,
    );
    assert_eq!(
        unknown.err(),
        Some(FaultStateFormatError::UnknownEnumeration)
    );
    Ok(())
}

#[test]
fn reservation_requires_the_next_ordinal_of_one_generation() -> Result<(), Box<dyn Error>> {
    let empty = FaultStateEnvelope::empty();
    let first = empty.reserved(entry(1, 1), [9; 32], state_limit(4))?;
    assert_eq!(first.len(), 1);
    assert_eq!(first.checkpoint_digest(), &[9; 32]);

    assert_eq!(
        first.reserved(entry(1, 1), [9; 32], state_limit(4)).err(),
        Some(FaultStateError::StaleReservation)
    );
    assert_eq!(
        first.reserved(entry(1, 3), [9; 32], state_limit(4)).err(),
        Some(FaultStateError::StaleReservation)
    );

    let second = first.reserved(entry(1, 2), [9; 32], state_limit(4))?;
    assert_eq!(second.reserved_ordinal(key(1)), Some(RetryOrdinal::new(2)?));
    assert_eq!(second.len(), 1);

    assert_eq!(
        second.reserved(entry(2, 1), [7; 32], state_limit(4)).err(),
        Some(FaultStateError::Corrupt(
            FaultStateFormatError::CheckpointMismatch
        ))
    );
    Ok(())
}

#[test]
fn capacity_and_retry_limits_are_bounded() -> Result<(), Box<dyn Error>> {
    let mut envelope = FaultStateEnvelope::empty();
    for seed in 1..=2 {
        envelope = envelope.reserved(entry(seed, 1), [9; 32], state_limit(2))?;
    }
    assert_eq!(
        envelope
            .reserved(entry(3, 1), [9; 32], state_limit(2))
            .err(),
        Some(FaultStateError::CapacityExhausted { max: 2 })
    );
    // An existing key keeps spending its own budget at capacity.
    let advanced = envelope.reserved(entry(2, 2), [9; 32], state_limit(2))?;

    assert_eq!(
        advanced.validate_for(retry_limit(2), state_limit(2), &[9; 32]),
        Ok(())
    );
    assert_eq!(
        advanced
            .validate_for(retry_limit(1), state_limit(2), &[9; 32])
            .err(),
        Some(FaultStateFormatError::OrdinalAboveLimit { max: 1 })
    );
    assert_eq!(
        advanced
            .validate_for(retry_limit(2), state_limit(1), &[9; 32])
            .err(),
        Some(FaultStateFormatError::TooManyEntries { max: 1 })
    );
    assert_eq!(
        advanced
            .validate_for(retry_limit(2), state_limit(2), &[8; 32])
            .err(),
        Some(FaultStateFormatError::CheckpointMismatch)
    );
    assert_eq!(
        FaultStateEnvelope::empty().validate_for(retry_limit(0), state_limit(1), &[8; 32]),
        Ok(())
    );
    Ok(())
}

#[test]
fn the_format_ceiling_holds_at_its_documented_bounds() -> Result<(), Box<dyn Error>> {
    assert_eq!(FaultStateEnvelope::MAX_ENTRIES, 256);
    assert_eq!(FaultStateEnvelope::MAX_BYTES, 64 * 1024);

    let full: Vec<_> = (0..FaultStateEnvelope::MAX_ENTRIES)
        .map(|index| {
            let mut digest = [0_u8; 32];
            digest[0] = u8::try_from(index).expect("index fits the ceiling");
            FaultStateEntry::new(
                RetryKey::from_bytes(digest),
                FaultPhase::Read,
                FailureCategory::UserComponent,
                RetryOrdinal::new(1).expect("ordinal fixture must be valid"),
                revision(),
            )
        })
        .collect();
    let envelope = FaultStateEnvelope::new([9; 32], full.clone())?;
    assert_eq!(envelope.len(), FaultStateEnvelope::MAX_ENTRIES);
    assert!(envelope.to_canonical_json()?.len() <= FaultStateEnvelope::MAX_BYTES);
    assert_eq!(reload(&envelope)?, envelope);

    let mut overflowing = full;
    overflowing.push(entry(255, 1));
    assert_eq!(
        FaultStateEnvelope::new([9; 32], overflowing).err(),
        Some(FaultStateFormatError::TooManyEntries { max: 256 })
    );
    Ok(())
}

#[test]
fn durable_state_retains_only_reviewed_members() -> Result<(), Box<dyn Error>> {
    let envelope = FaultStateEnvelope::new([9; 32], [entry(1, 1)])?;
    let payload: serde_json::Value = serde_json::from_slice(&envelope.to_canonical_json()?)?;
    let members: Vec<&str> = payload
        .as_object()
        .expect("canonical fault state is an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(members, vec!["checkpoint", "entries"]);

    let retained: Vec<&str> = payload["entries"][0]
        .as_object()
        .expect("a retained entry is an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        retained,
        vec!["category", "key", "ordinal", "phase", "revision"]
    );

    // The opaque key is restart-relevant persistence input, never a
    // diagnostic, so its `Debug` stays redacted even for an adapter author.
    assert!(format!("{:?}", envelope.entries()[0].key()).contains("redacted"));
    Ok(())
}
