//! What each dispatch form can express, and why erasure has to be a type.
//!
//! The contract's trait form is not dyn compatible on the supported
//! toolchain — that is exactly why the sealed object trait behind `Boxed*`
//! exists. These tests pin both halves of that: the compiler still rejects the
//! trait as a trait object, and the handle built on the sealed mirror restores
//! everything a registry needs.

#![allow(clippy::expect_used, clippy::items_after_statements, clippy::panic)]

use std::path::PathBuf;
use std::process::Command;

use oxide_batch::{ProcessContext, ProcessOutcome, ProcessorError, StopSource};
use oxide_batch_m6_spikes::contract::{BoxedProcessor, BoxedReader, BoxedWriter, ItemProcessor};
use oxide_batch_m6_spikes::workload::{
    ChecksumWriter, Output, RangeReader, Record, ScalingProcessor,
};

#[test]
fn the_contract_trait_form_is_not_dyn_compatible_on_the_supported_toolchain() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ui/native_rpitit_dyn.rs");
    let output = Command::new("rustc")
        .args(["--edition=2024", "--crate-type=lib"])
        .arg(&fixture)
        .arg("--out-dir")
        .arg(std::env::temp_dir())
        .output()
        .expect("rustc comparator must run");

    assert!(
        !output.status.success(),
        "the contract trait form unexpectedly compiled as a trait object"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not dyn compatible"),
        "unexpected compiler diagnostic: {stderr}"
    );
}

#[test]
fn the_boxed_handles_are_send_and_nameable() {
    let reader = BoxedReader::new(RangeReader::new(4));
    let processor = BoxedProcessor::new(ScalingProcessor::new(3));
    let writer = BoxedWriter::new(ChecksumWriter::new());

    fn assert_send<T: Send>(_: &T) {}
    assert_send(&reader);
    assert_send(&processor);
    assert_send(&writer);
}

/// A second concrete processor, so the registry below holds genuinely
/// different types rather than one type twice.
struct OffsetProcessor {
    offset: u64,
}

impl ItemProcessor<Record, Output> for OffsetProcessor {
    async fn process(
        &self,
        item: &Record,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<Output>, ProcessorError> {
        Ok(ProcessOutcome::Item(Output {
            id: item.id,
            payload: item.payload.wrapping_add(self.offset),
        }))
    }
}

#[tokio::test]
async fn one_handle_type_holds_a_heterogeneous_registry() {
    let (_source, stop) = StopSource::new();

    // No `dyn` appears in the type the application writes down.
    let registry: Vec<BoxedProcessor<Record, Output>> = vec![
        BoxedProcessor::new(ScalingProcessor::new(3)),
        BoxedProcessor::new(OffsetProcessor { offset: 11 }),
    ];

    let item = Record {
        id: 1,
        payload: 100,
    };
    let mut payloads = Vec::new();
    for processor in &registry {
        let outcome = processor
            .process(&item, ProcessContext::new(&stop))
            .await
            .expect("the registry processors must succeed");
        match outcome {
            ProcessOutcome::Item(output) => payloads.push(output.payload),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    assert_eq!(payloads, vec![300, 111]);
}
