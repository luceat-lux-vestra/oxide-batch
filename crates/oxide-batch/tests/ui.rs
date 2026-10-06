//! Compile-time coverage for public facade and item-component contracts.
//!
//! Positive ADR-0008 contracts are compiled as modules of this Cargo
//! integration-test crate. Integration tests are separate crates that consume
//! the public `oxide_batch` API, so these fixtures retain the public-consumer
//! type-checking boundary without starting a second trybuild runner.
//! Negative contracts remain in trybuild because compiler diagnostics are part
//! of their assertion.

#[allow(dead_code)]
#[path = "ui/item_reader_natural_async.rs"]
mod item_reader_natural_async;

#[allow(dead_code)]
#[path = "ui/item_reader_boxed_erasure.rs"]
mod item_reader_boxed_erasure;

#[allow(dead_code)]
#[path = "ui/item_reader_non_static_item.rs"]
mod item_reader_non_static_item;

/// M1/M5 facade leakage plus ADR-0008 negative contracts must remain compile
/// failures. One `TestCases` instance keeps trybuild to a single
/// preparation/execution cycle and a compile-fail-only workload.
#[test]
fn facade_exposes_no_runtime_database_or_telemetry_sdk_type() {
    let cases = trybuild::TestCases::new();

    // Public facade boundary guarantees.
    cases.compile_fail("tests/ui/executor_type_leakage.rs");
    cases.compile_fail("tests/ui/postgres_type_leakage.rs");
    cases.compile_fail("tests/ui/serializer_type_leakage.rs");
    cases.compile_fail("tests/ui/telemetry_type_leakage.rs");

    // ADR-0008 item-component contract guarantees.
    cases.compile_fail("tests/ui/item_reader_dyn_incompatible.rs");
    cases.compile_fail("tests/ui/item_processor_missing_impl.rs");
    cases.compile_fail("tests/ui/item_reader_non_send_body.rs");
}
