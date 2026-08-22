#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::zero_sized_map_values,
    clippy::items_after_statements
)]

//! Classifier-selected delegate contract tests (#146): keyed routing, missing
//! key, failure/stop propagation, and the heterogeneous-capability safety
//! case (5.5) -- a classifier can never advertise a stronger capability than
//! its least-capable delegate, because every delegate shares one Rust type.

use std::collections::HashMap;

use oxide_batch::item_components::{ClassifyingProcessor, ClassifyingWriter};
use oxide_batch::{
    BoxedProcessor, FailureCategory, ItemProcessor, ItemWriter, ProcessOutcome, ProcessorError,
    WriteOutcome, WriterError,
};
use oxide_batch_test::ComponentFixture;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum Kind {
    Fast,
    Slow,
}

struct FastPath;

impl ItemProcessor<i64, String> for FastPath {
    async fn process(
        &self,
        item: &i64,
        _context: oxide_batch::ProcessContext<'_>,
    ) -> Result<ProcessOutcome<String>, ProcessorError> {
        Ok(ProcessOutcome::Item(format!("fast:{item}")))
    }
}

/// A distinct concrete type from [`FastPath`], with different internal
/// state, to prove heterogeneous delegates route correctly once erased.
struct SlowPath {
    prefix: String,
}

impl ItemProcessor<i64, String> for SlowPath {
    async fn process(
        &self,
        item: &i64,
        _context: oxide_batch::ProcessContext<'_>,
    ) -> Result<ProcessOutcome<String>, ProcessorError> {
        Ok(ProcessOutcome::Item(format!("{}:{item}", self.prefix)))
    }
}

#[tokio::test]
async fn classifying_processor_routes_by_key() {
    let fixture = ComponentFixture::new();
    let mut delegates: HashMap<Kind, oxide_batch::item_components::IdentityProcessor> =
        HashMap::new();
    delegates.insert(Kind::Fast, oxide_batch::item_components::IdentityProcessor);
    // Only one homogeneous delegate type is meaningful here; the
    // heterogeneous case is covered below via `BoxedProcessor`.
    let classifier = ClassifyingProcessor::new(delegates, |_: &i64| Kind::Fast);
    assert_eq!(
        classifier.process(&7, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(7))
    );
}

#[tokio::test]
async fn classifying_processor_missing_key_is_a_typed_failure() {
    let fixture = ComponentFixture::new();
    let delegates: HashMap<Kind, oxide_batch::item_components::IdentityProcessor> = HashMap::new();
    let classifier = ClassifyingProcessor::new(delegates, |_: &i64| Kind::Fast);
    assert_eq!(
        classifier.process(&7, fixture.process_context()).await,
        Err(ProcessorError::new())
    );
}

#[tokio::test]
async fn classifying_processor_heterogeneous_delegates_share_one_erased_type() {
    let fixture = ComponentFixture::new();
    let mut delegates: HashMap<Kind, BoxedProcessor<i64, String>> = HashMap::new();
    delegates.insert(Kind::Fast, BoxedProcessor::new(FastPath));
    delegates.insert(
        Kind::Slow,
        BoxedProcessor::new(SlowPath {
            prefix: String::from("slow"),
        }),
    );
    // The delegate map's value type is `BoxedProcessor<i64, String>` for
    // *every* key: the classifier's static declaration (Send + Sync, erased
    // dispatch cost) is identical regardless of which key a given call
    // selects, because there is no distinct "FastPath capability" the type
    // system can see -- both variants were erased to the same type at
    // construction, which is the accepted ADR-0008 boundary, not a new one.
    let classifier = ClassifyingProcessor::new(delegates, |item: &i64| {
        if *item % 2 == 0 {
            Kind::Fast
        } else {
            Kind::Slow
        }
    });
    assert_eq!(
        classifier.process(&2, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(String::from("fast:2")))
    );
    assert_eq!(
        classifier.process(&3, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(String::from("slow:3")))
    );
}

#[tokio::test]
async fn classifying_processor_delegate_failure_propagates_unchanged() {
    let fixture = ComponentFixture::new();
    struct AlwaysFails;
    impl ItemProcessor<i64, i64> for AlwaysFails {
        async fn process(
            &self,
            _item: &i64,
            _context: oxide_batch::ProcessContext<'_>,
        ) -> Result<ProcessOutcome<i64>, ProcessorError> {
            Err(ProcessorError::with_category(FailureCategory::Timeout))
        }
    }
    let mut delegates: HashMap<Kind, AlwaysFails> = HashMap::new();
    delegates.insert(Kind::Fast, AlwaysFails);
    let classifier = ClassifyingProcessor::new(delegates, |_: &i64| Kind::Fast);
    assert_eq!(
        classifier.process(&1, fixture.process_context()).await,
        Err(ProcessorError::with_category(FailureCategory::Timeout))
    );
}

// ---------------------------------------------------------------------
// ClassifyingWriter: per-item routing preserves exact input order
// ---------------------------------------------------------------------

use std::sync::{Arc, Mutex};

struct RecordingWriter {
    label: &'static str,
    seen: Arc<Mutex<Vec<(&'static str, i64)>>>,
}

impl ItemWriter<i64> for RecordingWriter {
    async fn write(
        &self,
        items: &[i64],
        _context: oxide_batch::WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        let mut seen = self.seen.lock().unwrap();
        for item in items {
            seen.push((self.label, *item));
        }
        Ok(WriteOutcome::Written)
    }
}

#[tokio::test]
async fn classifying_writer_preserves_original_item_order_across_delegates() {
    let fixture = ComponentFixture::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut delegates: HashMap<Kind, RecordingWriter> = HashMap::new();
    delegates.insert(
        Kind::Fast,
        RecordingWriter {
            label: "fast",
            seen: Arc::clone(&seen),
        },
    );
    delegates.insert(
        Kind::Slow,
        RecordingWriter {
            label: "slow",
            seen: Arc::clone(&seen),
        },
    );
    let writer = ClassifyingWriter::new(delegates, |item: &i64| {
        if *item % 2 == 0 {
            Kind::Fast
        } else {
            Kind::Slow
        }
    });
    let batch = [1, 2, 3, 4, 5];
    assert_eq!(
        writer.write(&batch, fixture.write_context()).await,
        Ok(WriteOutcome::Written)
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            ("slow", 1),
            ("fast", 2),
            ("slow", 3),
            ("fast", 4),
            ("slow", 5),
        ],
        "each item must be written to its classified delegate in the batch's original order"
    );
}

#[tokio::test]
async fn classifying_writer_missing_key_is_a_typed_failure() {
    let fixture = ComponentFixture::new();
    let delegates: HashMap<Kind, RecordingWriter> = HashMap::new();
    let writer = ClassifyingWriter::new(delegates, |_: &i64| Kind::Fast);
    assert_eq!(
        writer.write(&[1], fixture.write_context()).await,
        Err(WriterError::new())
    );
}

#[tokio::test]
async fn classifying_writer_stop_short_circuits() {
    let fixture = ComponentFixture::new();
    fixture.request_stop();
    let delegates: HashMap<Kind, RecordingWriter> = HashMap::new();
    let writer = ClassifyingWriter::new(delegates, |_: &i64| Kind::Fast);
    assert_eq!(
        writer.write(&[1], fixture.write_context()).await,
        Ok(WriteOutcome::Stopped),
        "stop must be observed before any classification/lookup is attempted"
    );
}
