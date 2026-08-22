#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Contract tests for the basic, validator, and filter components (#146).
//!
//! Exercises production components through the public `oxide-batch-test`
//! surface (`ComponentFixture`), per #145's exit criterion.

use oxide_batch::item_components::{
    FilterProcessor, IdentityProcessor, IterReader, NoopWriter, ValidatingProcessor,
};
use oxide_batch::{
    ItemProcessor, ItemReader, ItemWriter, ProcessOutcome, ProcessorError, ReadOutcome,
    WriteOutcome,
};
use oxide_batch_test::ComponentFixture;

#[tokio::test]
async fn iter_reader_yields_items_then_end_of_input() {
    let fixture = ComponentFixture::new();
    let mut reader = IterReader::new(vec![1, 2, 3]);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(1))
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(2))
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(3))
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput)
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput),
        "end of input is stable"
    );
}

#[tokio::test]
async fn identity_processor_passes_items_through_unchanged() {
    let fixture = ComponentFixture::new();
    let processor = IdentityProcessor;
    assert_eq!(
        processor.process(&42u64, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(42))
    );
}

#[tokio::test]
async fn identity_processor_observes_stop() {
    let fixture = ComponentFixture::new();
    fixture.request_stop();
    let processor = IdentityProcessor;
    assert_eq!(
        processor.process(&42u64, fixture.process_context()).await,
        Ok(ProcessOutcome::Stopped)
    );
}

#[tokio::test]
async fn noop_writer_accepts_and_discards() {
    let fixture = ComponentFixture::new();
    let writer = NoopWriter;
    assert_eq!(
        writer.write(&[1, 2, 3], fixture.write_context()).await,
        Ok(WriteOutcome::Written)
    );
}

#[tokio::test]
async fn validating_processor_returns_typed_failure_for_invalid_item() {
    let fixture = ComponentFixture::new();
    let validator = ValidatingProcessor::new(|item: &i64| {
        if *item >= 0 {
            Ok(())
        } else {
            Err(ProcessorError::new())
        }
    });
    assert_eq!(
        validator.process(&5, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(5)),
        "a valid item passes through unchanged"
    );
    assert_eq!(
        validator.process(&-1, fixture.process_context()).await,
        Err(ProcessorError::new()),
        "validation failure is a typed processor error, not a panic or a silent filter"
    );
}

#[tokio::test]
async fn filter_processor_uses_filtered_outcome_not_error_or_sentinel() {
    let fixture = ComponentFixture::new();
    let evens = FilterProcessor::new(|item: &i64| item % 2 == 0);
    assert_eq!(
        evens.process(&4, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(4))
    );
    assert_eq!(
        evens.process(&5, fixture.process_context()).await,
        Ok(ProcessOutcome::Filtered),
        "an odd item is filtered, not errored or reported as a magic value"
    );
}
