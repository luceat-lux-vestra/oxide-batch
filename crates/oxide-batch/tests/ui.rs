//! Compile-fail coverage for facade boundary guarantees.

#[test]
fn facade_exposes_no_runtime_database_or_telemetry_sdk_type() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/executor_type_leakage.rs");
    cases.compile_fail("tests/ui/postgres_type_leakage.rs");
    cases.compile_fail("tests/ui/serializer_type_leakage.rs");
    cases.compile_fail("tests/ui/telemetry_type_leakage.rs");
}
