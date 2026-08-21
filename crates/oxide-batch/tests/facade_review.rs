//! Named scenarios for the M5 public facade and preview API review.
//!
//! The [M5 preview surface and disclosure gate](../../../docs/api/design-guidelines.md)
//! fixes what the preview may claim and what it may never disclose, and the
//! [design-gate evidence](../../../docs/project/m5-design-gate-evidence.md)
//! names the scenarios that review owes. Two of them live here:
//!
//! - the sensitive-payload sweep, which holds every prohibited payload class
//!   at once rather than one family at a time;
//! - the reviewed-surface reconciliation, which ties the committed snapshot to
//!   the enumeration the review record publishes.
//!
//! The other two named scenarios need a different runner and live where that
//! runner is. `facade_exposes_no_runtime_database_or_telemetry_sdk_type` is a
//! compile-fail scenario in `ui.rs`, because a re-export that does not exist
//! can only be observed as a type error.
//! `rustdoc_surface_contains_no_leaked_implementation_type` is
//! `cargo xtask surface`, because it needs a complete documentation build of
//! the facade and its dependencies; the
//! [facade review evidence](../../../docs/project/m5-facade-api-review-evidence.md)
//! records why it is a command rather than a test.

#![allow(clippy::expect_used)]

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use oxide_batch::{
    BusinessStatement, BusinessValue, Checkpoint, CodecId, CodecVersion, ComponentStateEnvelope,
    ComponentStreamIdentity, DefaultComponentCodec, DomainError, ExecutionContext, JobParameter,
    JobParameters, ParameterName, ParameterRole, ParameterValue, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, TaskletError,
    VersionedStateCodec, WriterError,
};
use serde_json::json;

/// A value no diagnostic may contain, chosen so a partial match is impossible.
const SENTINEL: &str = "oxide-batch-sentinel-disclosure-7b3e";

/// One reviewed class, how it withholds its value, and what it rendered.
type Rendered = (&'static str, Withheld, String);

/// How one class keeps a sensitive value out of its diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Withheld {
    /// The diagnostic names the value as withheld.
    ///
    /// Naming it matters for a value the type retains: an operator reading
    /// `<redacted>` knows a value exists and was suppressed, rather than that
    /// the field was empty.
    Marked,
    /// The diagnostic reports structure and renders no member.
    ///
    /// This is the shape for a container that describes its contents by count,
    /// and for an error that never retained the text in the first place.
    /// Neither has a value to mark.
    Omitted,
}

/// The reviewed preview surface, as the review record publishes it.
///
/// Each row is one re-export group of `src/lib.rs` and the number of names it
/// contributes. The totals are the review's enumeration: changing the surface
/// without revisiting the review fails here, which is the point.
const REVIEWED_SURFACE: &[(&str, usize)] = &[
    ("chunk", 32),
    ("chunk_runtime", 13),
    ("diagnostics", 9),
    ("fault", 2),
    ("fault_state", 11),
    ("flow", 20),
    ("item_listener", 12),
    ("item_stream", 11),
    ("listener", 7),
    ("oxide_batch_core", 105),
    ("oxide_batch_plan", 26),
    ("oxide_batch_repository", 110),
    ("repository", 14),
    ("runtime", 18),
    ("service", 7),
    ("shutdown", 20),
    ("telemetry", 38),
];

/// The names the reviewed surface exports outside a re-export group.
const REVIEWED_CONSTANTS: usize = 1;

/// The names the optional `postgres` feature contributes.
const REVIEWED_OPTIONAL: usize = 12;

#[test]
fn debug_output_redacts_every_sensitive_payload_class() -> Result<(), Box<dyn Error>> {
    let mut classes = vec![
        (
            "parameter name",
            Withheld::Marked,
            format!("{:?}", ParameterName::new(SENTINEL)?),
        ),
        (
            "parameter value",
            Withheld::Marked,
            format!("{:?}", ParameterValue::string(SENTINEL)?),
        ),
        (
            "parameter set",
            Withheld::Omitted,
            format!("{:?}", sensitive_parameters()?),
        ),
        (
            "checkpoint payload",
            Withheld::Marked,
            format!("{:?}", sensitive_checkpoint()?),
        ),
        (
            "context payload",
            Withheld::Marked,
            format!("{:?}", sensitive_context()?),
        ),
        (
            "component state payload",
            Withheld::Marked,
            format!("{:?}", sensitive_component_state()?),
        ),
        (
            "item value",
            Withheld::Marked,
            format!("{:?}", BusinessValue::text(SENTINEL)),
        ),
        (
            "statement text",
            Withheld::Marked,
            format!(
                "{:?}",
                BusinessStatement::new(SENTINEL, &[BusinessValue::text(SENTINEL)])
            ),
        ),
        (
            "component error text",
            Withheld::Omitted,
            format!(
                "{:?} {} {:?} {}",
                WriterError::from_error(UserFailure),
                WriterError::from_error(UserFailure),
                TaskletError::from_error(UserFailure),
                TaskletError::from_error(UserFailure),
            ),
        ),
    ];
    classes.extend(credential_classes()?);

    for (class, withheld, rendered) in &classes {
        assert!(
            !rendered.contains(SENTINEL),
            "the {class} class disclosed its value: {rendered}",
        );
        assert!(
            !rendered.is_empty(),
            "the {class} class rendered nothing, so its redaction proves nothing",
        );
        if *withheld == Withheld::Marked {
            assert!(
                rendered.contains("redacted"),
                "the {class} class must name the value as withheld: {rendered}",
            );
        }
    }

    Ok(())
}

#[test]
fn public_api_snapshot_matches_the_reviewed_preview_surface() -> Result<(), Box<dyn Error>> {
    let snapshot = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("facade")
            .join("public-api.txt"),
    )?;

    let entries: Vec<&str> = snapshot.lines().filter(|line| !line.is_empty()).collect();
    let optional = entries
        .iter()
        .filter(|entry| entry.ends_with("[postgres]"))
        .count();
    let reviewed: usize = REVIEWED_SURFACE
        .iter()
        .map(|(_, count)| count)
        .sum::<usize>()
        + REVIEWED_CONSTANTS;

    assert_eq!(
        entries.len(),
        reviewed,
        "the committed surface has {} names and the review enumerates {reviewed}; \
         revisit docs/project/m5-facade-api-review-evidence.md before rewriting \
         the snapshot",
        entries.len(),
    );
    assert_eq!(
        optional, REVIEWED_OPTIONAL,
        "the optional adapter surface is the only feature-gated boundary the \
         preview claims",
    );

    let delivered = groups(&fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("lib.rs"),
    )?);
    assert_eq!(
        delivered,
        REVIEWED_SURFACE
            .iter()
            .map(|(group, count)| ((*group).to_owned(), *count))
            .collect::<Vec<_>>(),
        "the review enumerates the surface by the group each name is delivered \
         through, so a moved name is reviewed where it now lives",
    );

    Ok(())
}

/// Counts the names each `pub use` group of the facade root contributes.
///
/// Groups are returned in declaration order, which `src/lib.rs` keeps sorted,
/// and a group declared twice under different feature gates is one entry.
fn groups(source: &str) -> Vec<(String, usize)> {
    let mut counted: Vec<(String, usize)> = Vec::new();
    let mut lines = source.lines().map(str::trim);

    while let Some(line) = lines.next() {
        let Some(rest) = line.strip_prefix("pub use ") else {
            continue;
        };
        let mut statement = rest.to_owned();
        while !statement.ends_with(';') {
            let Some(next) = lines.next() else { break };
            statement.push(' ');
            statement.push_str(next);
        }

        let Some((path, names)) = statement.trim_end_matches(';').split_once("::{") else {
            continue;
        };
        let count = names
            .trim_end_matches('}')
            .split(',')
            .filter(|name| !name.trim().is_empty())
            .count();

        match counted.iter_mut().find(|(group, _)| group == path) {
            Some((_, total)) => *total += count,
            None => counted.push((path.to_owned(), count)),
        }
    }

    counted
}

/// An application error whose text must never reach a framework diagnostic.
#[derive(Debug)]
struct UserFailure;

impl fmt::Display for UserFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(SENTINEL)
    }
}

impl Error for UserFailure {}

/// Builds a parameter set whose name and value are both sensitive.
fn sensitive_parameters() -> Result<JobParameters, DomainError> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new(SENTINEL)?,
        JobParameter::new(
            ParameterValue::string(SENTINEL)?,
            ParameterRole::Identifying,
        ),
    )?;
    Ok(parameters)
}

/// Retains a sentinel-bearing checkpoint payload the framework must withhold.
fn sensitive_checkpoint() -> Result<Checkpoint, Box<dyn Error>> {
    Ok(Checkpoint::from_json(
        &envelope("oxide-batch.checkpoint"),
        StateLimits::default(),
    )?)
}

/// Retains a sentinel-bearing context payload the framework must withhold.
fn sensitive_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        &envelope("oxide-batch.execution-context"),
        StateLimits::default(),
    )?)
}

/// Retains a sentinel-bearing component-state payload the framework must
/// withhold.
fn sensitive_component_state() -> Result<ComponentStateEnvelope, Box<dyn Error>> {
    struct SentinelCodec {
        schema: StateSchemaId,
    }
    impl VersionedStateCodec<String> for SentinelCodec {
        fn schema_id(&self) -> &StateSchemaId {
            &self.schema
        }
        fn current_version(&self) -> StateSchemaVersion {
            StateSchemaVersion::new(1).expect("nonzero")
        }
        fn encode(&self, value: &String) -> Result<Vec<u8>, StateCodecError> {
            serde_json::to_vec(&json!({ "value": value }))
                .map_err(|_| StateCodecError::InvalidPayload)
        }
        fn decode(&self, _payload: &[u8]) -> Result<String, StateCodecError> {
            Err(StateCodecError::InvalidPayload)
        }
    }
    let codec = DefaultComponentCodec::new(
        SentinelCodec {
            schema: StateSchemaId::new("test.sensitive")?,
        },
        CodecId::new("test.sensitive-codec")?,
        CodecVersion::new(1)?,
        RestartabilityDeclaration::Restartable,
    );
    Ok(ComponentStateEnvelope::encode(
        ComponentStreamIdentity::new("test.sensitive-stream")?,
        &String::from(SENTINEL),
        &codec,
        StateLimits::default(),
    )?)
}

/// Renders one recorded durable-state envelope carrying the sentinel.
fn envelope(format: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "format": format,
        "format_version": 1,
        "schema": "test.sensitive",
        "schema_version": 1,
        "payload": { "value": SENTINEL },
    }))
    .expect("a static envelope serializes")
}

/// Renders the credential classes the optional adapter contributes.
#[cfg(feature = "postgres")]
fn credential_classes() -> Result<Vec<Rendered>, Box<dyn Error>> {
    use oxide_batch::{CaCertificate, PostgresConfig, TlsMode};

    let connection_string = format!("postgres://runtime:{SENTINEL}@db.internal/metadata");
    let tls_mode = TlsMode::VerifyFull {
        ca_certificate: Some(CaCertificate::new(SENTINEL.as_bytes().to_vec())?),
    };

    Ok(vec![
        (
            "connection string",
            Withheld::Marked,
            format!("{:?}", PostgresConfig::new(connection_string)?),
        ),
        ("certificate", Withheld::Marked, format!("{tls_mode:?}")),
    ])
}

/// The credential classes are unreachable without the optional adapter.
#[cfg(not(feature = "postgres"))]
fn credential_classes() -> Result<Vec<Rendered>, Box<dyn Error>> {
    Ok(Vec::new())
}
