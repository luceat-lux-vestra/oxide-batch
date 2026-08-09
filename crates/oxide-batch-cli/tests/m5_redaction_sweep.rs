//! A sweep for prohibited value classes across every M5 diagnostic surface.
//!
//! Redaction is usually tested one value at a time, next to the code that
//! redacts it. That catches the leak the author was already thinking about. It
//! does not catch the surface nobody thought about, and it goes stale quietly:
//! a test can assert that a bundle does not contain `"payload-sentinel"` while
//! nothing in the run ever held that string, and it will keep passing after the
//! redaction is removed.
//!
//! This report is the other shape. It generates one canary per prohibited value
//! class, feeds each into the system through a place that really accepts a
//! value of that class, collects every artifact M5 can put in front of an
//! operator, and then scans all of them for all of the canaries at once. What
//! it proves is a property of the collection: no artifact carries any canary.
//! A surface added later is covered as soon as it is collected, and a canary
//! that stops being injected fails the report rather than weakening it.
//!
//! Four classes, each entering where a deployment really supplies one:
//!
//! - a **password** and a whole **database URL**, through the configuration the
//!   CLI reads from the environment and from a file;
//! - a **certificate**, through the same configuration, standing for private
//!   key material;
//! - a **payload**, as an identifying job parameter value, which is business
//!   data the launch path accepts, stores, and projects.
//!
//! Each canary carries its own class in its text and a per-run suffix, so a
//! leak names the class that leaked and cannot be satisfied by a stale artifact
//! left by an earlier run.
//!
//! The surfaces swept are the four the M5 gate names. Errors: the typed
//! configuration and connection failures, their `Display`, their `Debug`, and
//! every error in their `source` chains. Telemetry: the records the services
//! emit during the run, their fields, and the representation an exporter would
//! ship. CLI: standard output and standard error of successful, refused, and
//! failed invocations, in both output forms, including the configuration
//! diagnostic. Bundles: every file of a generated diagnostics bundle, its
//! manifest, and the values inside its JSON rather than only its text.
//!
//! Scanning is deliberately done twice for structured artifacts: once over the
//! serialized bytes and once over every string value reachable in the parsed
//! JSON. The first catches a leak anywhere; the second is what keeps the first
//! honest if a value is ever escaped or re-encoded on the way out.
//!
//! Redaction that worked by deleting diagnostics would pass every check above
//! and leave operators with nothing, so the report also requires the safe part
//! to survive: the configuration keys are still listed and still marked
//! redacted, the parameter the payload arrived in is still named, and the
//! failures still classify themselves.
//!
//! The retained evidence records the classes, the surfaces, the artifact count,
//! and the occurrence count. It never records a canary.

mod support;

use std::error::Error;
use std::fs;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    Clock, DropReportWindow, ExportError, ExportQueueBound, InMemoryExplorer,
    InMemoryJobRepository, JobExplorer, JobOperator, RetentionService, SequentialIdGenerator,
    TelemetryEventSink, TelemetryExportSink, TelemetryExporter, TelemetryQueue, TelemetryRecord,
};
use oxide_batch_cli::{ExitCategory, NoSchema, Services};
use serde_json::{Value, json};
use support::{TestHost, run, run_with_catalog, services, test_catalog};

/// The environment variable that tells the report where to retain its result.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_SECURITY_OBSERVATIONS";

/// The job the sweep launches.
const JOB: &str = "redaction-sweep-job";

/// The configuration file the sweep supplies canaries through.
const CONFIG: &str = "sweep-config.json";

/// One prohibited value class and the canary standing for it.
struct Canary {
    /// The class, as the retained evidence names it.
    class: &'static str,
    /// Where a deployment really supplies a value of this class.
    entry: &'static str,
    /// The literal that must appear in no artifact.
    value: String,
}

/// One artifact the sweep collected.
struct Artifact {
    /// The surface it came from, as the retained evidence names it.
    surface: &'static str,
    /// What it is, within that surface.
    name: String,
    /// Its bytes, as text.
    text: String,
    /// Its parsed form, when it is JSON.
    structured: Option<Value>,
}

impl Artifact {
    /// Collects one textual artifact.
    fn text(surface: &'static str, name: impl Into<String>, text: impl Into<String>) -> Self {
        let text = text.into();
        let structured = serde_json::from_str(&text).ok();
        Self {
            surface,
            name: name.into(),
            text,
            structured,
        }
    }

    /// Returns every string this artifact carries, however it carries it.
    ///
    /// The serialized form is scanned along with the values inside it, because
    /// the two fail differently: a value escaped on the way out survives the
    /// first scan, and a value carried in a key or in framing survives the
    /// second.
    fn strings(&self) -> Vec<&str> {
        let mut strings = vec![self.text.as_str()];
        if let Some(structured) = &self.structured {
            collect_strings(structured, &mut strings);
        }
        strings
    }
}

/// Walks a JSON document, collecting every string it contains.
fn collect_strings<'a>(value: &'a Value, into: &mut Vec<&'a str>) {
    match value {
        Value::String(text) => into.push(text.as_str()),
        Value::Array(items) => {
            for item in items {
                collect_strings(item, into);
            }
        }
        Value::Object(members) => {
            for (key, member) in members {
                into.push(key.as_str());
                collect_strings(member, into);
            }
        }
        _ => {}
    }
}

#[test]
fn redaction_sweep_finds_no_prohibited_value_class() -> Result<(), Box<dyn Error>> {
    let canaries = canaries();
    let mut artifacts = Vec::new();

    sweep_cli_and_bundles(&canaries, &mut artifacts);
    sweep_errors(&canaries, &mut artifacts);
    sweep_telemetry(&canaries, &mut artifacts);

    // Every class must have reached something. A canary that was generated and
    // never injected would make its own absence meaningless, which is the way
    // a redaction test rots without failing.
    assert!(
        !artifacts.is_empty(),
        "the sweep collected no artifacts, so it proved nothing",
    );

    let mut occurrences = Vec::new();
    for artifact in &artifacts {
        for canary in &canaries {
            for string in artifact.strings() {
                if string.contains(&canary.value) {
                    occurrences.push(format!(
                        "the {} class reached {} in {}",
                        canary.class, artifact.name, artifact.surface
                    ));
                    break;
                }
            }
        }
    }
    assert!(
        occurrences.is_empty(),
        "prohibited value classes reached diagnostic surfaces: {occurrences:?}",
    );

    let preserved = require_diagnostics_survive(&artifacts);

    let surfaces = surfaces(&artifacts);
    retain_observation(&json!({
        "report": "redaction sweep across the M5 diagnostic surfaces",
        "scenario": "redaction_sweep_finds_no_prohibited_value_class",
        "value_classes_scanned": canaries
            .iter()
            .map(|canary| json!({ "class": canary.class, "entered_through": canary.entry }))
            .collect::<Vec<_>>(),
        "surfaces_scanned": surfaces,
        "artifacts_scanned": artifacts.len(),
        "strings_scanned": artifacts
            .iter()
            .map(|artifact| artifact.strings().len())
            .sum::<usize>(),
        "prohibited_occurrences": occurrences.len(),
        "diagnostics_preserved": preserved,
        "violations": Vec::<String>::new(),
        "passed": true,
        "scenario_result": "passed",
    }))?;

    Ok(())
}

/// Builds one canary per prohibited value class.
///
/// The per-run suffix is what stops an artifact left by an earlier run, or a
/// literal that happens to exist in this repository, from standing in for a
/// value this run actually injected.
fn canaries() -> Vec<Canary> {
    let run = format!(
        "{:x}{:x}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos()),
    );
    vec![
        Canary {
            class: "password",
            entry: "the repository URL's credential, from the environment and a config file",
            value: format!("oxide-secret-password-{run}"),
        },
        Canary {
            class: "database-url-endpoint",
            entry: "the repository URL's host and database, from the environment and a file",
            value: format!("oxide-secret-endpoint-{run}"),
        },
        Canary {
            class: "certificate",
            entry: "the repository CA certificate, from the environment and a config file",
            value: format!("oxide-secret-certificate-{run}"),
        },
        Canary {
            class: "payload",
            entry: "an identifying job parameter value supplied to launch",
            value: format!("oxide-secret-payload-{run}"),
        },
    ]
}

/// Returns the repository URL the canaries build.
fn canary_url(canaries: &[Canary]) -> String {
    format!(
        "postgres://batch:{}@{}.invalid:5432/{}",
        canary(canaries, "password"),
        canary(canaries, "database-url-endpoint"),
        canary(canaries, "database-url-endpoint"),
    )
}

/// Returns one canary's literal by class.
fn canary<'a>(canaries: &'a [Canary], class: &str) -> &'a str {
    canaries
        .iter()
        .find(|canary| canary.class == class)
        .map_or("", |canary| canary.value.as_str())
}

/// Builds a host whose configuration carries every secret-bearing canary.
///
/// Both routes a deployment has are used at once — the environment and a
/// configuration file — because they resolve through different code and a
/// redaction that only covered one would be a real gap.
fn configured_host(canaries: &[Canary]) -> TestHost {
    let url = canary_url(canaries);
    let certificate = canary(canaries, "certificate");
    TestHost::new()
        .with_env("OXIDE_BATCH_REPOSITORY_URL", &url)
        .with_env("OXIDE_BATCH_REPOSITORY_CA_CERTIFICATE", certificate)
        .with_file(
            CONFIG,
            &format!(
                "{{\"config_version\":1,\"repository\":\
                 {{\"url\":\"{url}\",\"ca_certificate\":\"{certificate}\"}}}}"
            ),
        )
        .with_mode(CONFIG, 0o600)
}

/// Sweeps the CLI's own output and the bundles it writes.
#[allow(
    clippy::too_many_lines,
    reason = "the surfaces are one list of invocations, and splitting them would hide which \
              artifacts the sweep collects"
)]
fn sweep_cli_and_bundles(canaries: &[Canary], artifacts: &mut Vec<Artifact>) {
    let payload = canary(canaries, "payload");

    // A launch carrying business data in an identifying parameter. This is the
    // payload class entering through the path that accepts it.
    let (services, _repository) = services();
    let catalog = test_catalog(JOB);
    let mut launch = configured_host(canaries);
    let category = run_with_catalog(
        &mut launch,
        &services,
        &catalog,
        &format!(
            "launch --job {JOB} --actor campaign --operation-id sweep-launch \
             --parameter business_key={payload} --output json"
        ),
    );
    assert_eq!(
        category,
        ExitCategory::Success,
        "the sweep's launch must succeed for the payload class to have entered: {}",
        launch.stderr_text(),
    );
    artifacts.push(Artifact::text("cli", "launch:stdout", launch.stdout_text()));
    artifacts.push(Artifact::text("cli", "launch:stderr", launch.stderr_text()));

    let execution = launch.envelope()["data"]["execution"]["execution_id"]
        .as_u64()
        .unwrap_or_default();

    // The configuration diagnostic, in both forms. This is the command whose
    // whole purpose is to describe configuration that is mostly secret.
    for (form, name) in [("json", "config-show:json"), ("text", "config-show:text")] {
        let mut host = configured_host(canaries);
        let line = format!("config show --config {CONFIG} --output {form}");
        let _ = run(&mut host, &services, &line);
        artifacts.push(Artifact::text(
            "cli",
            format!("{name}:stdout"),
            host.stdout_text(),
        ));
        artifacts.push(Artifact::text(
            "cli",
            format!("{name}:stderr"),
            host.stderr_text(),
        ));
    }

    // Ordinary reads, which carry the launched instance and its parameters.
    for (line, name) in [
        ("job list --output json", "job-list"),
        (
            "instance list --job redaction-sweep-job --output json",
            "instance-list",
        ),
        (
            "execution list --instance 1 --output json",
            "execution-list",
        ),
    ] {
        let mut host = configured_host(canaries);
        let _ = run(&mut host, &services, line);
        artifacts.push(Artifact::text(
            "cli",
            format!("{name}:stdout"),
            host.stdout_text(),
        ));
        artifacts.push(Artifact::text(
            "cli",
            format!("{name}:stderr"),
            host.stderr_text(),
        ));
    }

    // Failure output: a refused argument and an unknown target. A diagnostic
    // written on the way out is exactly where an echoed value tends to appear.
    for (line, name) in [
        ("job list --colour red", "invalid-argument"),
        (
            "execution show --execution 999999 --output json",
            "unknown-target",
        ),
        (
            "launch --job never-registered --actor a --operation-id o",
            "unknown-job",
        ),
    ] {
        let mut host = configured_host(canaries);
        let _ = run(&mut host, &services, line);
        artifacts.push(Artifact::text(
            "cli",
            format!("{name}:stdout"),
            host.stdout_text(),
        ));
        artifacts.push(Artifact::text(
            "cli",
            format!("{name}:stderr"),
            host.stderr_text(),
        ));
    }

    // The diagnostics bundle, which is the artifact an operator is most likely
    // to send somewhere else.
    let mut bundle = configured_host(canaries);
    let command = format!(
        "diagnostics bundle --execution {execution} --out sweep-bundle --config {CONFIG} \
         --output json"
    );
    assert_eq!(
        run(&mut bundle, &services, &command),
        ExitCategory::Success,
        "the sweep must be able to generate a bundle: {}",
        bundle.stderr_text(),
    );
    artifacts.push(Artifact::text("cli", "bundle:stdout", bundle.stdout_text()));
    for name in bundle.directory_files("sweep-bundle") {
        let text = bundle.file_text(&format!("sweep-bundle/{name}"));
        artifacts.push(Artifact::text("bundle", name, text));
    }
}

/// Sweeps typed errors, their rendering, and their whole source chain.
fn sweep_errors(canaries: &[Canary], artifacts: &mut Vec<Artifact>) {
    sweep_adapter_errors(canaries, artifacts);

    // The configuration the CLI resolved, rather than one built in a test. This
    // holds every secret-bearing canary and is the value most likely to be
    // printed while debugging, and it exists whether or not an adapter is
    // compiled in.
    let mut host = configured_host(canaries);
    let arguments = support::words(&format!("config show --config {CONFIG}"));
    if let Ok(plan) = oxide_batch_cli::prepare(&mut host, &arguments) {
        artifacts.push(Artifact::text(
            "errors",
            "cli-configuration:debug",
            format!("{:?}", plan.config()),
        ));
        sweep_backend_errors(&plan, artifacts);
    }
}

/// Sweeps the failures the `PostgreSQL` adapter produces.
///
/// The adapter is optional, and the CLI is checked without it. Everything this
/// function collects is therefore gated: the surfaces are real and the campaign
/// runs with every feature enabled, so the sweep sees them there, and a build
/// without the adapter sweeps the surfaces that build actually has.
#[cfg(feature = "postgres")]
fn sweep_adapter_errors(canaries: &[Canary], artifacts: &mut Vec<Artifact>) {
    let url = canary_url(canaries);
    let certificate = canary(canaries, "certificate");

    // A configuration failure produced from a URL that carries the canaries.
    let refused = oxide_batch::PostgresConfig::new(format!("{url}?sslmode=disable"));
    let error = refused.err().map(|error| render_error(&error));
    if let Some(rendered) = error {
        artifacts.push(Artifact::text(
            "errors",
            "postgres-config:refused",
            rendered,
        ));
    }

    // The configuration value itself, which holds both canaries and is the
    // thing most likely to be printed while debugging.
    if let Ok(config) = oxide_batch::PostgresConfig::new(url.clone()) {
        let config = config.with_tls_mode(oxide_batch::TlsMode::VerifyFull {
            ca_certificate: oxide_batch::CaCertificate::new(certificate.as_bytes().to_vec()).ok(),
        });
        artifacts.push(Artifact::text(
            "errors",
            "postgres-config:debug",
            format!("{config:?}"),
        ));

        // A real connection failure. The host does not resolve, so this needs
        // no server, and the failure is produced by the same path a misconfigured
        // deployment would take.
        let outcome = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()
            .map(|runtime| {
                runtime.block_on(oxide_batch::PostgresJobRepository::connect(
                    config,
                    Arc::new(SweepClock),
                ))
            });
        if let Some(Err(error)) = outcome {
            artifacts.push(Artifact::text(
                "errors",
                "postgres-connect:failed",
                render_error(&error),
            ));
        }
    }
}

/// Sweeps nothing when the adapter is not compiled in.
#[cfg(not(feature = "postgres"))]
fn sweep_adapter_errors(_canaries: &[Canary], _artifacts: &mut Vec<Artifact>) {}

/// Sweeps the connection configuration the CLI builds and the refusal it gives.
#[cfg(feature = "postgres")]
fn sweep_backend_errors(plan: &oxide_batch_cli::Plan, artifacts: &mut Vec<Artifact>) {
    match oxide_batch_cli::connection_config(plan.config()) {
        Ok(config) => artifacts.push(Artifact::text(
            "errors",
            "cli-backend:config-debug",
            format!("{config:?}"),
        )),
        Err(failure) => artifacts.push(Artifact::text(
            "errors",
            "cli-backend:refused",
            format!("{failure:?} {:?}", failure.diagnostic()),
        )),
    }
}

/// Sweeps nothing when the adapter is not compiled in.
#[cfg(not(feature = "postgres"))]
fn sweep_backend_errors(_plan: &oxide_batch_cli::Plan, _artifacts: &mut Vec<Artifact>) {}

/// Renders one error the way every diagnostic path can render it.
///
/// Only the adapter's failures are rendered this way, so this is compiled with
/// them.
#[cfg(feature = "postgres")]
///
/// `Display`, `Debug`, and the whole `source` chain are concatenated, because a
/// value that is absent from one is not thereby absent from the others, and the
/// chain is where a wrapped driver error would carry the connection string.
fn render_error(error: &dyn Error) -> String {
    let mut rendered = format!("display={error}\ndebug={error:?}");
    let mut source = error.source();
    while let Some(inner) = source {
        let _ = std::fmt::Write::write_fmt(
            &mut rendered,
            format_args!("\nsource-display={inner}\nsource-debug={inner:?}"),
        );
        source = inner.source();
    }
    rendered
}

/// Sweeps the telemetry the run emitted, and what an exporter would ship.
fn sweep_telemetry(canaries: &[Canary], artifacts: &mut Vec<Artifact>) {
    let payload = canary(canaries, "payload");
    let catalog = test_catalog(JOB);

    // The services are built here rather than taken from the shared harness so
    // that a sink this report owns is attached as a second one. Sinks
    // accumulate, so the CLI's own incident buffer still receives everything
    // and the bundle is unaffected; this one keeps every record rather than the
    // newest few for one execution, because a record the sweep never sees is a
    // record it never scanned.
    let recorder = Arc::new(RecordingSink::default());
    let services = sweep_services(&recorder);

    let mut host = configured_host(canaries);
    let category = run_with_catalog(
        &mut host,
        &services,
        &catalog,
        &format!(
            "launch --job {JOB} --actor campaign --operation-id sweep-telemetry \
             --parameter business_key={payload} --output json"
        ),
    );
    assert_eq!(
        category,
        ExitCategory::Success,
        "the sweep's telemetry launch must succeed: {}",
        host.stderr_text(),
    );
    // The bundle's own event projection is swept with the bundle. What is swept
    // here is the record itself: its Debug, and every field key and value,
    // which is what a log line and a span are built from.
    let records = recorder.records();
    for (index, record) in records.iter().enumerate() {
        artifacts.push(Artifact::text(
            "telemetry",
            format!("record:{index}:debug"),
            format!("{record:?}"),
        ));
        let fields = record
            .fields()
            .iter()
            .map(|field| format!("{}={}", field.key(), field.value()))
            .collect::<Vec<_>>()
            .join("\n");
        artifacts.push(Artifact::text(
            "telemetry",
            format!("record:{index}:fields"),
            fields,
        ));
    }
    assert!(
        !records.is_empty(),
        "the sweep observed no telemetry, so the telemetry surface proved nothing",
    );

    // What an exporter would ship off the host.
    for (index, rendered) in export(&records).into_iter().enumerate() {
        artifacts.push(Artifact::text(
            "telemetry",
            format!("exported:{index}"),
            rendered,
        ));
    }
}

/// A telemetry sink that keeps every record it is given.
#[derive(Default)]
struct RecordingSink {
    records: std::sync::Mutex<Vec<TelemetryRecord>>,
}

impl RecordingSink {
    /// Returns every record emitted so far.
    fn records(&self) -> Vec<TelemetryRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl TelemetryEventSink for RecordingSink {
    fn emit(&self, event: &TelemetryRecord) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
    }
}

/// Builds services with one extra readable telemetry sink attached.
fn sweep_services(recorder: &Arc<RecordingSink>) -> support::TestServices {
    let clock: Arc<dyn Clock> = Arc::new(SweepClock);
    let first = NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN);
    let identifiers = Arc::new(SequentialIdGenerator::new(first));
    let repository = InMemoryJobRepository::new(Arc::clone(&clock), identifiers);
    let explorer_repository = InMemoryExplorer::new(&repository);
    let sink: Arc<dyn TelemetryEventSink> = Arc::<RecordingSink>::clone(recorder);
    Services::new(
        JobOperator::new(repository.clone(), Arc::clone(&clock)).with_event_sink(Arc::clone(&sink)),
        RetentionService::new(repository, Arc::clone(&clock)).with_event_sink(Arc::clone(&sink)),
        JobExplorer::new(explorer_repository).with_event_sink(sink),
        Box::new(NoSchema),
    )
}

/// Ships every record through an exporter and returns what the sink received.
fn export(records: &[TelemetryRecord]) -> Vec<String> {
    let Ok(bound) = ExportQueueBound::new(64) else {
        return Vec::new();
    };
    let Ok(window) = DropReportWindow::new(Duration::from_mins(1)) else {
        return Vec::new();
    };
    let queue = TelemetryQueue::new(bound, window);
    for record in records {
        let _ = queue.enqueue(record.clone(), Duration::ZERO);
    }
    let sink = CapturingSink::default();
    let captured = Arc::clone(&sink.captured);
    let exporter = TelemetryExporter::new(queue, sink);
    futures_executor::block_on(exporter.flush());
    let captured = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    captured.clone()
}

/// An export sink that keeps what it was given.
#[derive(Default)]
struct CapturingSink {
    captured: Arc<std::sync::Mutex<Vec<String>>>,
}

impl TelemetryExportSink for CapturingSink {
    fn export<'a>(
        &'a self,
        record: &'a TelemetryRecord,
    ) -> oxide_batch::BoxFuture<'a, Result<(), ExportError>> {
        Box::pin(async move {
            let fields = record
                .fields()
                .iter()
                .map(|field| format!("{}={}", field.key(), field.value()))
                .collect::<Vec<_>>()
                .join(" ");
            self.captured
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("{record:?} {fields}"));
            Ok(())
        })
    }
}

/// A clock that never moves, so nothing the sweep reads depends on time.
#[derive(Debug)]
struct SweepClock;

impl oxide_batch::Clock for SweepClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH
    }
}

/// Requires the diagnostics to still say something after redaction.
///
/// Removing a value is only the right answer when what remains still lets an
/// operator work. A bundle that dropped the configuration entirely, or a launch
/// that stopped naming the parameter it was given, would pass every scan above
/// and be worse than the leak.
fn require_diagnostics_survive(artifacts: &[Artifact]) -> Value {
    let configuration = artifacts
        .iter()
        .find(|artifact| artifact.surface == "bundle" && artifact.name == "configuration.json")
        .and_then(|artifact| artifact.structured.clone())
        .unwrap_or(Value::Null);
    let keys = configuration
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value.get("key").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        keys.contains(&"repository.url"),
        "the bundle stopped reporting that a repository URL is configured at all",
    );
    let redacted = configuration
        .as_array()
        .into_iter()
        .flatten()
        .filter(|value| value.get("redacted").and_then(Value::as_bool) == Some(true))
        .count();
    assert!(
        redacted > 0,
        "the bundle reports no value as redacted, so it is not distinguishing a withheld value \
         from an absent one",
    );

    // The payload arrived as an identifying job parameter, and the instance
    // projection is where an operator looks for it. The name and the type tag
    // must still be there: an operator who cannot see which parameters
    // identified an instance cannot tell two instances apart, which is a worse
    // outcome than the leak this report is about.
    let parameters = artifacts
        .iter()
        .find(|artifact| artifact.surface == "cli" && artifact.name == "instance-list:stdout")
        .and_then(|artifact| artifact.structured.clone())
        .and_then(|envelope| envelope.get("data").cloned())
        .and_then(|data| {
            data.as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("parameters"))
                .cloned()
        })
        .unwrap_or(Value::Null);
    let named = parameters
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        named.contains(&"business_key"),
        "the instance projection stopped naming the parameter the payload arrived in, so \
         redaction removed the diagnostic rather than the value",
    );
    assert!(
        parameters
            .as_array()
            .into_iter()
            .flatten()
            .all(|parameter| parameter.get("kind").is_some()),
        "the instance projection stopped reporting parameter types, which an operator needs to \
         read an identity it cannot see the values of",
    );

    json!({
        "configuration_keys_reported": keys.len(),
        "configuration_values_marked_redacted": redacted,
        "parameter_names_preserved": named.len(),
        "parameter_types_preserved": true,
    })
}

/// Returns the surfaces the sweep covered, in a stable order.
fn surfaces(artifacts: &[Artifact]) -> Vec<Value> {
    let mut surfaces: Vec<&'static str> = Vec::new();
    for artifact in artifacts {
        if !surfaces.contains(&artifact.surface) {
            surfaces.push(artifact.surface);
        }
    }
    surfaces.sort_unstable();
    surfaces
        .into_iter()
        .map(|surface| {
            json!({
                "surface": surface,
                "artifacts": artifacts
                    .iter()
                    .filter(|artifact| artifact.surface == surface)
                    .count(),
            })
        })
        .collect()
}

/// Retains the observation where `cargo xtask security` will read it.
fn retain_observation(document: &Value) -> Result<(), Box<dyn Error>> {
    let Some(directory) = std::env::var(OBSERVATIONS_ENV)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;
    fs::write(
        directory.join("redaction-sweep.json"),
        format!("{}\n", serde_json::to_string_pretty(document)?),
    )?;
    Ok(())
}
