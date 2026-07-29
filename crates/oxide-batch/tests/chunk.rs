//! Checked M2 chunk size, count, and redaction values.

use oxide_batch::{
    BusinessStatement, BusinessValue, ChunkCount, ChunkCounts, ChunkError, ChunkProgress,
    ChunkSize, StateError, StateLimits, StateSchemaId, StateSchemaVersion,
};

#[test]
fn chunk_size_and_arithmetic_reject_zero_overflow_and_invalid_state() -> Result<(), ChunkError> {
    assert_eq!(ChunkSize::new(0), Err(ChunkError::ZeroSize));
    assert_eq!(
        ChunkCount::new(u64::MAX).checked_increment(),
        Err(ChunkError::CountOverflow)
    );
    assert_eq!(
        ChunkCounts::new(
            ChunkCount::new(1),
            ChunkCount::new(1),
            ChunkCount::new(1),
            ChunkCount::new(1),
        ),
        Err(ChunkError::ClassifiedExceedsRead)
    );
    assert_eq!(
        ChunkCounts::new(
            ChunkCount::new(2),
            ChunkCount::new(1),
            ChunkCount::new(2),
            ChunkCount::ZERO,
        ),
        Err(ChunkError::WrittenExceedsProcessed)
    );

    let size = ChunkSize::new(1)?;
    let mut progress = ChunkProgress::new(size);
    progress.record_read()?;
    assert_eq!(progress.record_read(), Err(ChunkError::SizeExceeded));
    assert_eq!(
        progress.record_written(ChunkCount::new(1)),
        Err(ChunkError::WrittenExceedsProcessed)
    );
    progress.record_filtered()?;
    assert_eq!(
        progress.record_processed(),
        Err(ChunkError::ClassifiedExceedsRead)
    );
    Ok(())
}

#[test]
fn state_configuration_is_typed_and_bounded() {
    assert_eq!(StateSchemaId::new(""), Err(StateError::EmptySchemaId));
    assert_eq!(
        StateSchemaVersion::new(0),
        Err(StateError::ZeroSchemaVersion)
    );
    assert_eq!(
        StateLimits::new(0, 16),
        Err(StateError::InvalidByteLimit { maximum: 1_048_576 })
    );
    assert_eq!(
        StateLimits::new(64 * 1024, 0),
        Err(StateError::InvalidDepthLimit { maximum: 64 })
    );
    let defaults = StateLimits::default();
    assert_eq!(defaults.maximum_bytes(), 64 * 1024);
    assert_eq!(defaults.maximum_depth(), 16);
}

#[test]
fn business_statement_debug_redacts_sql_and_values() {
    let secret = "sentinel-secret-record";
    let values = [BusinessValue::text(secret), BusinessValue::i64(42)];
    let statement = BusinessStatement::new("INSERT sentinel SQL", &values);
    let diagnostics = format!("{statement:?}\n{:?}", values[0]);

    assert!(!diagnostics.contains(secret));
    assert!(!diagnostics.contains("INSERT sentinel SQL"));
    assert!(diagnostics.contains("<redacted>"));
}
