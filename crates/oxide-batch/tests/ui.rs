//! Compile-fail coverage for facade boundary guarantees.

#[test]
fn facade_exposes_no_runtime_database_or_telemetry_sdk_type() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/executor_type_leakage.rs");
    cases.compile_fail("tests/ui/postgres_type_leakage.rs");
    cases.compile_fail("tests/ui/serializer_type_leakage.rs");
    cases.compile_fail("tests/ui/telemetry_type_leakage.rs");
}

/// ADR-0008: the item component contract compiles the way it is documented —
/// natural `async fn` impls satisfy it, `Boxed*` is the only supported
/// erasure, and the contract traits are not meant to be named as `dyn Trait`.
#[test]
fn item_component_contract_matches_its_documented_shape() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/item_reader_natural_async.rs");
    cases.pass("tests/ui/item_reader_boxed_erasure.rs");
    cases.compile_fail("tests/ui/item_reader_dyn_incompatible.rs");
    cases.compile_fail("tests/ui/item_processor_missing_impl.rs");
}
