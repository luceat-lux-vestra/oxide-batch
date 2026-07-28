//! Execution-context evolution evidence.

#![allow(clippy::expect_used)]

use oxide_batch_m0_spikes::context::{
    ContextError, ContextLimits, InventoryContextSchema, decode_context, encode_context,
};
use serde_json::Value;

const V1: &[u8] = include_bytes!("fixtures/context/inventory-v1.json");
const V2_MISSING_OPTIONAL: &[u8] =
    include_bytes!("fixtures/context/inventory-v2-missing-optional.json");
const V2_ADDITIVE: &[u8] = include_bytes!("fixtures/context/inventory-v2-additive.json");
const CORRUPTED: &[u8] = include_bytes!("fixtures/context/corrupted.json");

#[test]
fn v1_fixture_upgrades_to_current_schema() {
    let context = decode_context(V1, &InventoryContextSchema, ContextLimits::default())
        .expect("v1 fixture must upgrade");

    assert_eq!(context.cursor, 41);
    assert_eq!(context.source_checksum, None);
    assert_eq!(
        context.extensions.get("partition"),
        Some(&Value::String(String::from("north")))
    );
}

#[test]
fn missing_optional_field_uses_a_documented_default() {
    let context = decode_context(
        V2_MISSING_OPTIONAL,
        &InventoryContextSchema,
        ContextLimits::default(),
    )
    .expect("missing optional field must default");

    assert_eq!(context.cursor, 42);
    assert_eq!(context.source_checksum, None);
}

#[test]
fn additive_unknown_fields_survive_read_and_rewrite() {
    let limits = ContextLimits::default();
    let context = decode_context(V2_ADDITIVE, &InventoryContextSchema, limits)
        .expect("additive fixture must decode");
    let rewritten =
        encode_context(&context, &InventoryContextSchema, limits).expect("context must encode");
    let round_trip = decode_context(&rewritten, &InventoryContextSchema, limits)
        .expect("rewritten context must decode");

    assert_eq!(
        round_trip.extensions.get("future_hint"),
        Some(&Value::String(String::from("retain-me")))
    );
}

#[test]
fn corrupted_payload_has_a_stable_redacted_diagnostic() {
    let error = decode_context(CORRUPTED, &InventoryContextSchema, ContextLimits::default())
        .expect_err("corrupted fixture must fail");

    assert_eq!(error, ContextError::Malformed);
    assert_eq!(error.to_string(), "execution context is malformed");
    assert!(!error.to_string().contains("secret-fixture-value"));
}

#[test]
fn newer_framework_and_application_versions_are_rejected() {
    let newer_format = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":2,
        "schema":"inventory-import",
        "schema_version":2,
        "payload":{"cursor":1}
    }"#;
    let newer_schema = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":3,
        "payload":{"cursor":1}
    }"#;

    assert_eq!(
        decode_context(
            newer_format,
            &InventoryContextSchema,
            ContextLimits::default()
        ),
        Err(ContextError::UnsupportedFormatVersion)
    );
    assert_eq!(
        decode_context(
            newer_schema,
            &InventoryContextSchema,
            ContextLimits::default()
        ),
        Err(ContextError::UnsupportedSchemaVersion)
    );
}

#[test]
fn size_depth_and_type_limits_fail_before_context_use() {
    let limits = ContextLimits {
        maximum_bytes: 32,
        maximum_depth: 4,
    };
    assert_eq!(
        decode_context(V1, &InventoryContextSchema, limits),
        Err(ContextError::TooLarge)
    );

    let deep = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":2,
        "payload":{"cursor":1,"nested":{"one":{"two":true}}}
    }"#;
    assert_eq!(
        decode_context(
            deep,
            &InventoryContextSchema,
            ContextLimits {
                maximum_bytes: 1024,
                maximum_depth: 4,
            }
        ),
        Err(ContextError::TooDeep)
    );

    let wrong_type = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":2,
        "payload":{"cursor":"forty-two"}
    }"#;
    assert_eq!(
        decode_context(
            wrong_type,
            &InventoryContextSchema,
            ContextLimits::default()
        ),
        Err(ContextError::InvalidPayload)
    );
}
