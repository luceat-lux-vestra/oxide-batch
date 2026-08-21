//! Named evidence for the M6 `#144` component-state envelope (Gate C).
//!
//! Scenarios cover the [`ComponentStateEnvelope`] contract independent of the
//! chunk runtime: identity/namespace, the checksum-before-decode boundary,
//! the two distinct migration axes (application schema vs. codec), bounds,
//! sensitivity/disclosure, restartability declaration, and the bounded
//! inline/external payload boundary. Chunk-commit and `PostgreSQL`
//! crash/restart evidence for the same envelope live in
//! `item_stream.rs`/`postgres_item_stream_crash_recovery.rs`.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicUsize, Ordering};

use oxide_batch::{
    CodecId, CodecVersion, CodecVersionUpgrade, ComponentStateCodec, ComponentStateEnvelope,
    ComponentStateError, ComponentStatePayload, ComponentStreamIdentity, ContentIdentity,
    DefaultComponentCodec, ExternalStateReference, RestartabilityDeclaration, StateCodecError,
    StateLimits, StateSchemaId, StateSchemaUpgrade, StateSchemaVersion, StateSensitivity,
    VersionedStateCodec,
};
use serde_json::{Map, Value, json};

/// The typed component state the fixture codec below round-trips.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Counter {
    total: u64,
}

/// A declared schema-upgrade edge before it is validated into a
/// [`StateSchemaUpgrade`].
type DeclaredSchemaEdge = (u32, u32, fn(&[u8]) -> Result<Vec<u8>, StateCodecError>);

/// A schema whose current version renamed `count` to `total`.
struct CounterSchema {
    schema: StateSchemaId,
    current: StateSchemaVersion,
    upgrades: Vec<StateSchemaUpgrade>,
}

impl CounterSchema {
    fn new(current: u32, upgrades: Vec<DeclaredSchemaEdge>) -> Self {
        let declared = upgrades
            .into_iter()
            .map(|(from, to, apply)| {
                StateSchemaUpgrade::new(
                    StateSchemaVersion::new(from).expect("nonzero"),
                    StateSchemaVersion::new(to).expect("nonzero"),
                    apply,
                )
                .expect("increasing edge")
            })
            .collect();
        Self {
            schema: StateSchemaId::new("test.component.counter").expect("valid schema id"),
            current: StateSchemaVersion::new(current).expect("nonzero"),
            upgrades: declared,
        }
    }
}

impl VersionedStateCodec<Counter> for CounterSchema {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }

    fn current_version(&self) -> StateSchemaVersion {
        self.current
    }

    fn upgrades(&self) -> &[StateSchemaUpgrade] {
        &self.upgrades
    }

    fn encode(&self, value: &Counter) -> Result<Vec<u8>, StateCodecError> {
        // Version 1 recorded the field as `count`; version 2 renamed it to
        // `total`. Encoding at an older declared version must produce the
        // shape that version actually wrote, so the declared upgrade has
        // something real to rename.
        let field = if self.current.get() >= 2 {
            "total"
        } else {
            "count"
        };
        serde_json::to_vec(&json!({ field: value.total }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<Counter, StateCodecError> {
        let value: Map<String, Value> =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let total = value
            .get("total")
            .and_then(Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        Ok(Counter { total })
    }
}

static SCHEMA_UPGRADES: AtomicUsize = AtomicUsize::new(0);
static CODEC_UPGRADES: AtomicUsize = AtomicUsize::new(0);

fn rename_count_to_total(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
    SCHEMA_UPGRADES.fetch_add(1, Ordering::Relaxed);
    let mut value: Map<String, Value> =
        serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
    let count = value
        .remove("count")
        .ok_or(StateCodecError::InvalidPayload)?;
    value.insert(String::from("total"), count);
    serde_json::to_vec(&Value::Object(value)).map_err(|_| StateCodecError::InvalidPayload)
}

// The signature is fixed by `CodecVersionUpgrade::apply`; this fixture's
// upgrade happens not to fail, but the type must still match the contract.
#[allow(clippy::unnecessary_wraps)]
fn codec_upgrade_noop(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
    CODEC_UPGRADES.fetch_add(1, Ordering::Relaxed);
    Ok(payload.to_vec())
}

/// Builds the fully capable codec: schema/codec current version 2, with the
/// complete declared upgrade chain reaching version 2 from version 1 on both
/// axes. This is the "current release" codec used to decode/resolve
/// recorded state of any supported version.
fn full_codec(restartability: RestartabilityDeclaration) -> DefaultComponentCodec<CounterSchema> {
    let schema = CounterSchema::new(
        2,
        vec![(
            1,
            2,
            rename_count_to_total as fn(&[u8]) -> Result<Vec<u8>, StateCodecError>,
        )],
    );
    DefaultComponentCodec::new(
        schema,
        CodecId::new("test.codec.counter").expect("valid codec id"),
        CodecVersion::new(2).expect("nonzero"),
        restartability,
    )
    .with_codec_upgrades(vec![
        CodecVersionUpgrade::new(
            CodecVersion::new(1).expect("nonzero"),
            CodecVersion::new(2).expect("nonzero"),
            codec_upgrade_noop,
        )
        .expect("increasing edge"),
    ])
}

/// Builds a codec pinned at an explicit schema/codec version pair with no
/// upgrade declarations of its own -- used only to *produce* durable bytes
/// exactly as a component at that historical (or future) version would have
/// written them, never to resolve/decode.
fn writer_codec(
    schema_version: u32,
    codec_version: u32,
    restartability: RestartabilityDeclaration,
) -> DefaultComponentCodec<CounterSchema> {
    DefaultComponentCodec::new(
        CounterSchema::new(schema_version, vec![]),
        CodecId::new("test.codec.counter").expect("valid codec id"),
        CodecVersion::new(codec_version).expect("nonzero"),
        restartability,
    )
}

fn namespace() -> ComponentStreamIdentity {
    ComponentStreamIdentity::new("reader.counter").expect("valid namespace")
}

fn current_codec() -> DefaultComponentCodec<CounterSchema> {
    full_codec(RestartabilityDeclaration::Restartable)
}

/// Round-trips an envelope produced by an older codec version through
/// [`ComponentStateEnvelope::from_durable`] paired with the current codec,
/// simulating a durable row a real adapter would have read back unchanged.
fn recorded_by(
    older: &DefaultComponentCodec<CounterSchema>,
    value: &Counter,
) -> ComponentStateEnvelope {
    let envelope =
        ComponentStateEnvelope::encode(namespace(), value, older, StateLimits::default())
            .expect("older codec encodes");
    let payload = envelope.payload().expect("payload readable");
    ComponentStateEnvelope::from_durable(
        namespace(),
        envelope.schema_id().as_str(),
        envelope.schema_version().get(),
        envelope.codec_id().as_str(),
        envelope.codec_version().get(),
        envelope.checksum_algorithm(),
        envelope.checksum_algorithm_version(),
        envelope.checksum(),
        payload,
        StateLimits::default(),
    )
    .expect("older envelope reconstructs")
}

#[test]
fn equal_component_state_version_decodes_without_migration() {
    SCHEMA_UPGRADES.store(0, Ordering::Relaxed);
    CODEC_UPGRADES.store(0, Ordering::Relaxed);
    let codec = current_codec();
    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 7 },
        &codec,
        StateLimits::default(),
    )
    .expect("encodes");
    let decoded: Counter = envelope.decode(&codec).expect("decodes");
    assert_eq!(decoded, Counter { total: 7 });
    assert_eq!(SCHEMA_UPGRADES.load(Ordering::Relaxed), 0);
    assert_eq!(CODEC_UPGRADES.load(Ordering::Relaxed), 0);
}

#[test]
fn older_component_state_upgrades_through_one_directed_chain() {
    SCHEMA_UPGRADES.store(0, Ordering::Relaxed);
    CODEC_UPGRADES.store(0, Ordering::Relaxed);
    let older = writer_codec(1, 1, RestartabilityDeclaration::Restartable);
    let recorded = recorded_by(&older, &Counter { total: 41 });

    let decoded: Counter = recorded
        .decode(&current_codec())
        .expect("upgrades and decodes");

    assert_eq!(decoded, Counter { total: 41 });
    assert_eq!(SCHEMA_UPGRADES.load(Ordering::Relaxed), 1);
    assert_eq!(CODEC_UPGRADES.load(Ordering::Relaxed), 1);
}

#[test]
fn newer_component_state_version_is_rejected() {
    let older_current = current_codec();
    let newer_recorded = writer_codec(3, 2, RestartabilityDeclaration::Restartable);
    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 1 },
        &newer_recorded,
        StateLimits::default(),
    )
    .expect("encodes");

    let error = envelope
        .decode(&older_current)
        .expect_err("newer schema version must fail closed");
    assert!(matches!(
        error,
        ComponentStateError::UnsupportedSchemaVersion {
            found: 3,
            current: 2
        }
    ));
}

#[test]
fn unknown_component_state_schema_is_rejected() {
    // Same codec identity/version, but a schema the resolving codec does not
    // declare -- decode must reject it rather than guess compatibility.
    struct OtherSchemaCodec {
        schema: StateSchemaId,
    }
    impl VersionedStateCodec<Counter> for OtherSchemaCodec {
        fn schema_id(&self) -> &StateSchemaId {
            &self.schema
        }
        fn current_version(&self) -> StateSchemaVersion {
            StateSchemaVersion::new(2).expect("nonzero")
        }
        fn encode(&self, value: &Counter) -> Result<Vec<u8>, StateCodecError> {
            serde_json::to_vec(&json!({ "total": value.total }))
                .map_err(|_| StateCodecError::InvalidPayload)
        }
        fn decode(&self, payload: &[u8]) -> Result<Counter, StateCodecError> {
            let value: Map<String, Value> =
                serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
            let total = value
                .get("total")
                .and_then(Value::as_u64)
                .ok_or(StateCodecError::InvalidPayload)?;
            Ok(Counter { total })
        }
    }
    let recorded_with = current_codec();
    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 1 },
        &recorded_with,
        StateLimits::default(),
    )
    .expect("encodes");

    let other = DefaultComponentCodec::new(
        OtherSchemaCodec {
            schema: StateSchemaId::new("test.component.other").expect("valid schema id"),
        },
        CodecId::new("test.codec.counter").expect("valid codec id"),
        CodecVersion::new(2).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    );

    let error = envelope
        .decode(&other)
        .expect_err("unrecognized schema id must fail closed");
    assert!(matches!(error, ComponentStateError::SchemaMismatch));
}

#[test]
fn unknown_component_state_codec_is_rejected() {
    let codec = current_codec();
    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 1 },
        &codec,
        StateLimits::default(),
    )
    .expect("encodes");
    let other_codec = DefaultComponentCodec::new(
        CounterSchema::new(2, vec![]),
        CodecId::new("test.codec.other").expect("valid codec id"),
        CodecVersion::new(2).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    );
    let error = envelope
        .decode(&other_codec)
        .expect_err("unrecognized codec id must fail closed");
    assert!(matches!(error, ComponentStateError::UnknownCodec));
}

#[test]
fn missing_migration_path_is_rejected() {
    // A codec whose current version is 2 but declares no edge reaching it
    // from 1 -- the chain stalls immediately.
    let gapped = DefaultComponentCodec::new(
        CounterSchema::new(2, vec![]),
        CodecId::new("test.codec.counter").expect("valid codec id"),
        CodecVersion::new(2).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    );
    let older = writer_codec(2, 1, RestartabilityDeclaration::Restartable);
    let recorded = recorded_by(&older, &Counter { total: 9 });

    let error = recorded
        .decode(&gapped)
        .expect_err("missing codec upgrade path must fail closed");
    assert!(matches!(
        error,
        ComponentStateError::NoCodecUpgradePath {
            found: 1,
            current: 2
        }
    ));
}

#[test]
fn checksum_is_verified_before_decode() {
    let codec = current_codec();
    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 5 },
        &codec,
        StateLimits::default(),
    )
    .expect("encodes");
    let ComponentStatePayload::Inline(mut bytes) = envelope.payload().expect("payload readable")
    else {
        panic!("fixture always encodes inline");
    };
    // Flip one payload byte without recomputing the checksum.
    bytes[0] ^= 0xFF;

    let error = ComponentStateEnvelope::from_durable(
        namespace(),
        envelope.schema_id().as_str(),
        envelope.schema_version().get(),
        envelope.codec_id().as_str(),
        envelope.codec_version().get(),
        envelope.checksum_algorithm(),
        envelope.checksum_algorithm_version(),
        envelope.checksum(),
        ComponentStatePayload::Inline(bytes),
        StateLimits::default(),
    )
    .expect_err("tampered payload must fail the checksum check");

    assert!(matches!(error, ComponentStateError::ChecksumMismatch));
}

#[test]
fn corrupt_component_state_is_rejected_without_decode() {
    // A codec whose `encode` returns non-JSON bytes: malformed at the
    // structural boundary, never reaching a codec `decode` call.
    struct MalformedCodec {
        schema_id: StateSchemaId,
        schema_version: StateSchemaVersion,
    }
    impl VersionedStateCodec<Counter> for MalformedCodec {
        fn schema_id(&self) -> &StateSchemaId {
            &self.schema_id
        }
        fn current_version(&self) -> StateSchemaVersion {
            self.schema_version
        }
        fn encode(&self, _value: &Counter) -> Result<Vec<u8>, StateCodecError> {
            Ok(b"not json".to_vec())
        }
        fn decode(&self, _payload: &[u8]) -> Result<Counter, StateCodecError> {
            panic!("decode must not run for structurally malformed state");
        }
    }
    let codec = DefaultComponentCodec::new(
        MalformedCodec {
            schema_id: StateSchemaId::new("test.component.counter").expect("valid schema id"),
            schema_version: StateSchemaVersion::new(2).expect("nonzero"),
        },
        CodecId::new("test.codec.counter").expect("valid codec id"),
        CodecVersion::new(2).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    );

    let error = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 0 },
        &codec,
        StateLimits::default(),
    )
    .expect_err("non-JSON payload bytes must be rejected before decode");
    assert!(matches!(
        error,
        ComponentStateError::InvalidPayload | ComponentStateError::PayloadNotObject
    ));
}

#[test]
fn oversized_component_state_is_rejected() {
    let codec = current_codec();
    let tiny = StateLimits::new(16, 16).expect("valid limits");
    let error =
        ComponentStateEnvelope::encode(namespace(), &Counter { total: 123_456_789 }, &codec, tiny)
            .expect_err("payload larger than the configured limit must be rejected");
    assert!(matches!(error, ComponentStateError::TooLarge { .. }));
}

#[test]
fn overdeep_component_state_is_rejected() {
    struct DeepCodec;
    impl VersionedStateCodec<()> for DeepCodec {
        fn schema_id(&self) -> &StateSchemaId {
            static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| StateSchemaId::new("test.deep").expect("valid schema id"))
        }
        fn current_version(&self) -> StateSchemaVersion {
            StateSchemaVersion::new(1).expect("nonzero")
        }
        fn encode(&self, (): &()) -> Result<Vec<u8>, StateCodecError> {
            let mut value = json!({ "leaf": true });
            for _ in 0..20 {
                value = json!({ "nested": value });
            }
            serde_json::to_vec(&value).map_err(|_| StateCodecError::InvalidPayload)
        }
        fn decode(&self, _payload: &[u8]) -> Result<(), StateCodecError> {
            panic!("decode must not run for over-deep state");
        }
    }
    let codec = DefaultComponentCodec::new(
        DeepCodec,
        CodecId::new("test.codec.deep").expect("valid codec id"),
        CodecVersion::new(1).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    );
    let error = ComponentStateEnvelope::encode(namespace(), &(), &codec, StateLimits::default())
        .expect_err("payload deeper than the configured limit must be rejected");
    assert!(matches!(error, ComponentStateError::TooDeep { .. }));
}

#[test]
fn sensitive_component_state_never_reaches_diagnostics() {
    const SENTINEL: &str = "oxide-batch-sentinel-component-state-7f3a";
    struct SentinelCodec;
    impl VersionedStateCodec<String> for SentinelCodec {
        fn schema_id(&self) -> &StateSchemaId {
            static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| StateSchemaId::new("test.sentinel").expect("valid schema id"))
        }
        fn current_version(&self) -> StateSchemaVersion {
            StateSchemaVersion::new(1).expect("nonzero")
        }
        fn encode(&self, value: &String) -> Result<Vec<u8>, StateCodecError> {
            serde_json::to_vec(&json!({ "value": value }))
                .map_err(|_| StateCodecError::InvalidPayload)
        }
        fn decode(&self, payload: &[u8]) -> Result<String, StateCodecError> {
            let value: Map<String, Value> =
                serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
            value
                .get("value")
                .and_then(Value::as_str)
                .map(String::from)
                .ok_or(StateCodecError::InvalidPayload)
        }
    }
    let codec = DefaultComponentCodec::new(
        SentinelCodec,
        CodecId::new("test.codec.sentinel").expect("valid codec id"),
        CodecVersion::new(1).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    )
    .with_sensitivity(StateSensitivity::Sensitive);

    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &String::from(SENTINEL),
        &codec,
        StateLimits::default(),
    )
    .expect("encodes");

    // The sentinel really is retained (or this scenario proves nothing).
    let ComponentStatePayload::Inline(bytes) = envelope.payload().expect("payload readable") else {
        panic!("fixture always encodes inline");
    };
    assert!(String::from_utf8_lossy(&bytes).contains(SENTINEL));

    // But it must never appear in the envelope's own Debug output.
    let debug = format!("{envelope:?}");
    assert!(!debug.contains(SENTINEL));
    assert!(debug.contains("<redacted>"));
    assert!(debug.contains("schema_id"));
    assert!(debug.contains("codec_id"));
}

#[test]
fn corrupt_sensitive_state_never_reaches_diagnostics() {
    const SENTINEL: &str = "oxide-batch-sentinel-corrupt-9c14";
    let codec = current_codec();
    let envelope = ComponentStateEnvelope::encode(
        namespace(),
        &Counter { total: 1 },
        &codec,
        StateLimits::default(),
    )
    .expect("encodes");
    let ComponentStatePayload::Inline(_) = envelope.payload().expect("payload readable") else {
        panic!("fixture always encodes inline");
    };
    // Embed a sentinel in tampered bytes so it is present in the input but
    // must not surface via the resulting error.
    let bytes = format!("{{\"total\": \"{SENTINEL}\"").into_bytes();

    let error = ComponentStateEnvelope::from_durable(
        namespace(),
        envelope.schema_id().as_str(),
        envelope.schema_version().get(),
        envelope.codec_id().as_str(),
        envelope.codec_version().get(),
        envelope.checksum_algorithm(),
        envelope.checksum_algorithm_version(),
        envelope.checksum(),
        ComponentStatePayload::Inline(bytes),
        StateLimits::default(),
    )
    .expect_err("tampered payload must fail closed");

    let rendered = format!("{error:?} / {error}");
    assert!(!rendered.contains(SENTINEL));
}

#[test]
fn stateful_nonpersistent_component_cannot_claim_restartability() {
    let codec = full_codec(RestartabilityDeclaration::NotRestartable);
    // A reader checkpoint being present elsewhere in the same step is
    // irrelevant to this component's own declaration -- the two are
    // independent, never conflated.
    assert!(matches!(
        codec.restartability(),
        RestartabilityDeclaration::NotRestartable
    ));
}

#[test]
fn reconstructible_or_persisted_state_can_satisfy_restartability() {
    let codec = full_codec(RestartabilityDeclaration::Restartable);
    assert!(matches!(
        codec.restartability(),
        RestartabilityDeclaration::Restartable
    ));
}

#[test]
fn oversized_state_is_not_silently_inlined() {
    let codec = current_codec();
    let tiny = StateLimits::new(8, 16).expect("valid limits");
    let error =
        ComponentStateEnvelope::encode(namespace(), &Counter { total: 999_999_999 }, &codec, tiny)
            .expect_err("oversized candidate must fail rather than silently succeed");
    assert!(matches!(error, ComponentStateError::TooLarge { .. }));
}

#[test]
fn external_state_reference_is_content_identified_and_bounded() {
    let blob = b"large external component state payload".to_vec();
    let reference = ExternalStateReference::new(ContentIdentity::of(&blob), blob.len() as u64);
    assert!(reference.verify(&blob).is_ok());

    let envelope = ComponentStateEnvelope::external(
        namespace(),
        StateSchemaId::new("test.component.counter").expect("valid schema id"),
        StateSchemaVersion::new(2).expect("nonzero"),
        CodecId::new("test.codec.counter").expect("valid codec id"),
        CodecVersion::new(2).expect("nonzero"),
        reference,
    );
    assert!(envelope.is_external());
    assert_eq!(envelope.encoded_len(), blob.len());

    // Reconstructing from durable columns still checksum-validates first.
    let payload = envelope.payload().expect("payload readable");
    let reconstructed = ComponentStateEnvelope::from_durable(
        namespace(),
        envelope.schema_id().as_str(),
        envelope.schema_version().get(),
        envelope.codec_id().as_str(),
        envelope.codec_version().get(),
        envelope.checksum_algorithm(),
        envelope.checksum_algorithm_version(),
        envelope.checksum(),
        payload,
        StateLimits::default(),
    )
    .expect("external envelope reconstructs");
    assert!(reconstructed.is_external());
}

#[test]
fn content_identity_mismatch_is_rejected() {
    let blob = b"the real external bytes".to_vec();
    let other = b"substituted bytes of the same shape".to_vec();
    let reference = ExternalStateReference::new(ContentIdentity::of(&blob), blob.len() as u64);

    let error = reference
        .verify(&other)
        .expect_err("resolved bytes must match the declared content identity");
    assert!(matches!(
        error,
        ComponentStateError::ExternalReferenceContentMismatch
    ));
}
