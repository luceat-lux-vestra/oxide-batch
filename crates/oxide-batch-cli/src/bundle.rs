//! Bounded, redacted diagnostics bundle generation.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use oxide_batch::{
    ExplorerError, ExplorerRepository, IncidentEventBuffer, JobExecutionId, JobExplorer,
    PageRequest, PageSize, RepositoryError, TelemetryRecord,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::config::Configuration;
use crate::host::Host;
use crate::project;
use crate::run::SchemaReport;

/// Diagnostics bundle format version.
pub(crate) const BUNDLE_FORMAT_VERSION: u16 = 1;
/// Maximum total encoded bundle bytes.
pub(crate) const MAX_BUNDLE_BYTES: usize = 4 * 1024 * 1024;
const MANIFEST_RESERVE_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
struct FileEntry {
    name: String,
    bytes: Vec<u8>,
    checksum: String,
}

/// One generated bundle ready for atomic writing.
#[derive(Clone, Debug)]
pub(crate) struct DiagnosticBundle {
    files: Vec<(String, Vec<u8>)>,
    manifest_checksum: String,
    total_bytes: usize,
}

impl DiagnosticBundle {
    pub(crate) fn files(&self) -> &[(String, Vec<u8>)] {
        &self.files
    }

    pub(crate) fn manifest_checksum(&self) -> &str {
        &self.manifest_checksum
    }

    pub(crate) const fn total_bytes(&self) -> usize {
        self.total_bytes
    }
}

/// A redacted diagnostics-bundle failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BundleError {
    TargetNotFound,
    Explorer(ExplorerError),
    Encoding,
    Write,
}

impl fmt::Display for BundleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TargetNotFound => formatter.write_str("the execution does not exist"),
            Self::Explorer(error) => error.fmt(formatter),
            Self::Encoding => {
                formatter.write_str("the redacted diagnostics bundle could not be encoded")
            }
            Self::Write => {
                formatter.write_str("the diagnostics bundle target could not be written")
            }
        }
    }
}

impl Error for BundleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Explorer(error) => Some(error),
            _ => None,
        }
    }
}

/// Builds and writes one new execution-scoped diagnostics directory.
pub(crate) async fn write<H, S>(
    host: &mut H,
    target: &std::path::Path,
    config: &Configuration,
    explorer: &JobExplorer<S>,
    schema: &dyn SchemaReport,
    events: &Arc<IncidentEventBuffer>,
    execution_id: JobExecutionId,
) -> Result<DiagnosticBundle, BundleError>
where
    H: Host,
    S: ExplorerRepository,
{
    let bundle = build(config, explorer, schema, events, execution_id).await?;
    host.write_new_directory(target, bundle.files())
        .map_err(|_| BundleError::Write)?;
    Ok(bundle)
}

#[allow(clippy::too_many_lines)]
async fn build<S>(
    config: &Configuration,
    explorer: &JobExplorer<S>,
    schema: &dyn SchemaReport,
    events: &Arc<IncidentEventBuffer>,
    execution_id: JobExecutionId,
) -> Result<DiagnosticBundle, BundleError>
where
    S: ExplorerRepository,
{
    let execution = explorer
        .get_execution(execution_id)
        .await
        .map_err(BundleError::Explorer)?
        .ok_or(BundleError::TargetNotFound)?;
    let page = PageRequest::first(PageSize::new(200).map_err(|_| BundleError::Encoding)?);
    let steps = explorer
        .list_step_executions(execution_id, &page)
        .await
        .map_err(BundleError::Explorer)?;
    let flow = explorer
        .list_flow_decisions(execution_id, &page)
        .await
        .map_err(BundleError::Explorer)?;
    let recovery = explorer
        .list_recovery_decisions(execution_id, &page)
        .await
        .map_err(BundleError::Explorer)?;
    let operator = explorer
        .list_operator_requests(execution_id, &page)
        .await
        .map_err(BundleError::Explorer)?;
    let mut partitions = Vec::new();
    let mut partition_truncated = false;
    for step in steps.rows() {
        let page_result = explorer
            .list_step_partitions(step.id(), &page)
            .await
            .map_err(BundleError::Explorer)?;
        partition_truncated |= page_result.next_cursor().is_some();
        partitions.extend(page_result.rows().iter().map(project::partition));
    }

    let schema_state = match schema.schema_state().await {
        Ok(state) => json!({
            "installed": state.installed,
            "supported": state.supported,
            "migration_required": state.migration_required(),
            "newer_than_supported": state.newer_than_supported(),
        }),
        Err(RepositoryError::UnsupportedCapability { .. }) => json!({
            "installed": Value::Null,
            "supported": Value::Null,
            "status": "not_applicable",
        }),
        Err(_) => json!({
            "installed": Value::Null,
            "supported": Value::Null,
            "status": "unavailable",
        }),
    };
    let configured = config
        .effective()
        .into_iter()
        .map(|value| {
            json!({
                "key": value.key(),
                "value": value.value(),
                "source": value.source().as_str(),
                "redacted": value.is_redacted(),
            })
        })
        .collect::<Vec<_>>();
    let retained_events = events
        .events_for(execution_id)
        .iter()
        .map(event_projection)
        .collect::<Vec<_>>();

    let mut omissions = Vec::new();
    if steps.next_cursor().is_some() {
        omissions.push("step_executions:record_limit".to_owned());
    }
    if flow.next_cursor().is_some() {
        omissions.push("flow_decisions:record_limit".to_owned());
    }
    if recovery.next_cursor().is_some() {
        omissions.push("recovery_decisions:record_limit".to_owned());
    }
    if operator.next_cursor().is_some() {
        omissions.push("operator_requests:record_limit".to_owned());
    }
    if partition_truncated {
        omissions.push("partitions:record_limit".to_owned());
    }

    let mut builder = BundleBuilder::new(omissions);
    builder.add("configuration.json", &json!(configured))?;
    builder.add(
        "repository.json",
        &json!({
            "adapter": "portable_repository",
            "schema": schema_state,
        }),
    )?;
    builder.add("execution.json", &project::execution(&execution))?;
    builder.add(
        "step-executions.json",
        &Value::Array(steps.rows().iter().map(project::step).collect()),
    )?;
    builder.add("partitions.json", &Value::Array(partitions))?;
    builder.add(
        "flow-decisions.json",
        &Value::Array(flow.rows().iter().map(project::flow_decision).collect()),
    )?;
    builder.add(
        "recovery-decisions.json",
        &Value::Array(
            recovery
                .rows()
                .iter()
                .map(project::recovery_decision)
                .collect(),
        ),
    )?;
    builder.add(
        "operator-requests.json",
        &Value::Array(
            operator
                .rows()
                .iter()
                .map(project::operator_record)
                .collect(),
        ),
    )?;
    builder.add("events.json", &Value::Array(retained_events))?;
    builder.add(
        "host.json",
        &json!({
            "cpu_count": std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            "available_memory_class": "unavailable",
            "platform": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
        }),
    )?;
    builder.finish(
        execution
            .definition()
            .map(oxide_batch::DefinitionDescriptor::manifest_format),
        &schema_state,
    )
}

fn event_projection(event: &TelemetryRecord) -> Value {
    json!({
        "schema_version": event.schema_version(),
        "name": event.kind().as_str(),
        "severity": event.kind().severity().as_str(),
        "timing": format!("{:?}", event.kind().timing()).to_ascii_lowercase(),
        "fields": event.fields().iter().map(|field| {
            json!({ "key": field.key(), "value": field.value() })
        }).collect::<Vec<_>>(),
    })
}

struct BundleBuilder {
    files: Vec<FileEntry>,
    omissions: Vec<String>,
    payload_bytes: usize,
}

impl BundleBuilder {
    fn new(omissions: Vec<String>) -> Self {
        Self {
            files: Vec::new(),
            omissions,
            payload_bytes: 0,
        }
    }

    fn add(&mut self, name: &str, value: &Value) -> Result<(), BundleError> {
        let mut bytes = serde_json::to_vec_pretty(&value).map_err(|_| BundleError::Encoding)?;
        bytes.push(b'\n');
        if self.payload_bytes.saturating_add(bytes.len())
            > MAX_BUNDLE_BYTES.saturating_sub(MANIFEST_RESERVE_BYTES)
        {
            self.omissions.push(format!("{name}:size_bound"));
            return Ok(());
        }
        self.payload_bytes = self.payload_bytes.saturating_add(bytes.len());
        self.files.push(FileEntry {
            name: name.to_owned(),
            checksum: checksum(&bytes),
            bytes,
        });
        Ok(())
    }

    fn finish(
        mut self,
        manifest_format: Option<u16>,
        schema_state: &Value,
    ) -> Result<DiagnosticBundle, BundleError> {
        self.files.sort_by(|left, right| left.name.cmp(&right.name));
        self.omissions.sort();
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |value| value.as_secs());
        let manifest_format = manifest_format.map_or(Value::Null, |value| json!(value));
        let manifest = json!({
            "bundle_format_version": BUNDLE_FORMAT_VERSION,
            "framework_version": oxide_batch::VERSION,
            "telemetry_schema_version": oxide_batch::TELEMETRY_SCHEMA_VERSION,
            "metadata_schema": schema_state,
            "manifest_format_version": manifest_format,
            "creation_instant": created_at,
            "files": self.files.iter().map(|file| {
                json!({
                    "name": file.name,
                    "bytes": file.bytes.len(),
                    "sha256": file.checksum,
                })
            }).collect::<Vec<_>>(),
            "omissions": self.omissions,
        });
        let mut manifest_bytes =
            serde_json::to_vec_pretty(&manifest).map_err(|_| BundleError::Encoding)?;
        manifest_bytes.push(b'\n');
        let manifest_checksum = checksum(&manifest_bytes);
        let total_bytes = self.payload_bytes.saturating_add(manifest_bytes.len());
        if total_bytes > MAX_BUNDLE_BYTES {
            return Err(BundleError::Encoding);
        }
        let mut files = Vec::with_capacity(self.files.len() + 1);
        files.push(("manifest.json".to_owned(), manifest_bytes));
        files.extend(self.files.into_iter().map(|file| (file.name, file.bytes)));
        Ok(DiagnosticBundle {
            files,
            manifest_checksum,
            total_bytes,
        })
    }
}

fn checksum(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = fmt::Write::write_fmt(&mut encoded, format_args!("{byte:02x}"));
    }
    encoded
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn diagnostic_bundle_respects_its_size_bound_and_records_omissions() {
        let mut builder = BundleBuilder::new(Vec::new());
        builder
            .add("small.json", &json!({ "status": "safe" }))
            .expect("small projection encodes");
        builder
            .add(
                "oversized.json",
                &json!({ "payload": "x".repeat(MAX_BUNDLE_BYTES) }),
            )
            .expect("oversized content is omitted rather than encoded");
        let bundle = builder
            .finish(Some(2), &json!({ "installed": 3 }))
            .expect("bounded bundle finishes");
        assert!(bundle.total_bytes() <= MAX_BUNDLE_BYTES);
        let manifest = bundle
            .files()
            .iter()
            .find(|(name, _)| name == "manifest.json")
            .map(|(_, bytes)| bytes)
            .expect("manifest exists");
        let manifest: Value = serde_json::from_slice(manifest).expect("manifest is JSON");
        assert!(
            manifest["omissions"]
                .as_array()
                .expect("omissions are an array")
                .iter()
                .any(|value| value == "oversized.json:size_bound")
        );
        assert!(
            !bundle
                .files()
                .iter()
                .any(|(name, _)| name == "oversized.json")
        );
    }
}
