//! Compile-fail coverage for facade boundary guarantees.

#[test]
fn public_facade_does_not_reexport_executor_or_postgres_driver_types() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/executor_type_leakage.rs");
    cases.compile_fail("tests/ui/postgres_type_leakage.rs");
}
