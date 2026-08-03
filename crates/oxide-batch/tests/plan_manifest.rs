//! Canonical manifest golden vectors, format reading, and migration behavior.

#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    BoxFuture, ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ChunkSize,
    ComponentRevision, DefinitionIdentity, DefinitionManifest, DefinitionRevision,
    DefinitionUpgrade, DefinitionUpgradeKey, FlowGraph, FlowNode, FlowTarget,
    InMemoryJobRepository, JobLauncher, JobName, JobParameters, JobRepository, ManifestError,
    RepositoryError, StateSchemaId, StateSchemaVersion, StepComponents, StepDefinitionUpgrade,
    StepName, StepNode, StopSource, Tasklet, TaskletContext, TaskletError, TaskletJob,
    TaskletOutcome, TaskletStep, TerminalKind,
};

const GOLDEN_MANIFEST: &[u8] =
    include_bytes!("fixtures/LIFE-DEFINITION-001/format2-two-step.manifest.json");
const GOLDEN_FINGERPRINT: &str = "c0ea69669657cb8ec425801588a1f042608d8785333ad7d38d8a1f7ed5d8557f";

fn hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;

    digest.iter().fold(String::new(), |mut rendered, byte| {
        let _ = write!(rendered, "{byte:02x}");
        rendered
    })
}

fn tasklet_node(id: &str) -> Result<FlowNode, Box<dyn Error>> {
    Ok(FlowNode::step(StepNode::new(
        oxide_batch::NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    )))
}

fn chunk_revisions() -> Result<ChunkComponentRevisions, Box<dyn Error>> {
    Ok(ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        ChunkRestartContract::new(
            StateSchemaId::new("test.position")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("test.context")?,
            StateSchemaVersion::new(1)?,
            ChunkDeliveryMode::AtomicSameResource,
        ),
    ))
}

/// The graph the committed golden vector pins.
fn golden_graph() -> Result<FlowGraph, Box<dyn Error>> {
    let load = oxide_batch::NodeId::new("load")?;
    let report = oxide_batch::NodeId::new("report")?;
    Ok(FlowGraph::new(load.clone())
        .with_node(FlowNode::step(StepNode::new(
            load.clone(),
            StepName::new("load")?,
            StepComponents::Chunk {
                size: ChunkSize::new(2)?,
                revisions: Box::new(chunk_revisions()?),
            },
        )))
        .with_node(tasklet_node("report")?)
        .with_sequence(load, FlowTarget::Node(report.clone()))?
        .with_sequence(report, FlowTarget::Terminal(TerminalKind::Complete))?)
}

fn golden_plan() -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    Ok(golden_graph()?.compile(
        &JobName::new("daily_import")?,
        DefinitionRevision::new("plan-v1")?,
    )?)
}

#[test]
fn format2_manifest_has_golden_bytes_and_fingerprint() -> Result<(), Box<dyn Error>> {
    let plan = golden_plan()?;

    assert_eq!(plan.manifest_format(), 2);
    assert_eq!(
        std::str::from_utf8(plan.definition_identity().canonical_manifest())?,
        std::str::from_utf8(GOLDEN_MANIFEST)?.trim_end_matches('\n')
    );
    assert_eq!(hex(plan.fingerprint()), GOLDEN_FINGERPRINT);
    Ok(())
}

#[test]
fn the_golden_manifest_reads_back_as_a_bounded_flow_manifest() -> Result<(), Box<dyn Error>> {
    let plan = golden_plan()?;
    let manifest = DefinitionManifest::read_verified(
        plan.definition_identity().canonical_manifest(),
        plan.fingerprint(),
    )?;

    assert_eq!(manifest.format(), 2);
    assert_eq!(manifest.node_count(), Some(2));
    assert_eq!(manifest.transition_count(), Some(4));
    assert_eq!(
        manifest.job_name().map(JobName::as_str),
        Some("daily_import")
    );
    Ok(())
}

#[test]
fn a_format2_runtime_still_reads_format1_manifests() -> Result<(), Box<dyn Error>> {
    let identity = DefinitionIdentity::tasklet(
        &JobName::new("daily_import")?,
        &StepName::new("import")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let manifest = DefinitionManifest::read_verified(
        identity.canonical_manifest(),
        identity.manifest_digest(),
    )?;

    assert_eq!(manifest.format(), 1);
    assert_eq!(manifest.node_count(), None);
    assert_eq!(manifest.transition_count(), None);
    Ok(())
}

#[test]
fn a_newer_manifest_is_rejected_rather_than_guessed() {
    let newer = br#"{"entry":"load","format":4,"job":"daily_import","nodes":[],"transitions":[]}"#;
    assert_eq!(
        DefinitionManifest::read(newer),
        Err(ManifestError::UnsupportedFormat {
            format: 4,
            supported: 3,
        })
    );
}

#[test]
fn malformed_and_non_canonical_manifests_fail_closed() {
    assert_eq!(
        DefinitionManifest::read(b"{"),
        Err(ManifestError::MalformedJson)
    );
    assert_eq!(
        DefinitionManifest::read(b"[]"),
        Err(ManifestError::NotAnObject)
    );
    // Members out of canonical byte order.
    assert_eq!(
        DefinitionManifest::read(br#"{"job":"daily_import","format":1}"#),
        Err(ManifestError::NonCanonicalEncoding)
    );
    // A repeated key silently collapses, so the bytes cannot be canonical.
    assert_eq!(
        DefinitionManifest::read(br#"{"format":1,"format":1}"#),
        Err(ManifestError::NonCanonicalEncoding)
    );
    assert_eq!(
        DefinitionManifest::read(br#"{"format":1,"ratio":1.5}"#),
        Err(ManifestError::FloatValue)
    );
    assert_eq!(
        DefinitionManifest::read(br#"{"kind":"tasklet"}"#),
        Err(ManifestError::MissingFormat)
    );
    assert_eq!(
        DefinitionManifest::read(br#"{"format":2,"job":"daily_import"}"#),
        Err(ManifestError::MalformedGraph)
    );
}

#[test]
fn an_altered_manifest_does_not_match_its_fingerprint() -> Result<(), Box<dyn Error>> {
    let plan = golden_plan()?;
    let mut altered = plan.definition_identity().canonical_manifest().to_vec();
    let position = altered
        .iter()
        .position(|byte| *byte == b'2')
        .ok_or("the golden manifest must contain a digit to alter")?;
    altered[position] = b'3';

    assert_eq!(
        DefinitionManifest::read_verified(&altered, plan.fingerprint()),
        Err(ManifestError::DigestMismatch)
    );
    Ok(())
}

#[test]
fn plan_diagnostics_do_not_print_manifest_bytes() -> Result<(), Box<dyn Error>> {
    let plan = golden_plan()?;
    let diagnostic = format!("{:?}", plan.definition_identity());

    assert!(diagnostic.contains("digest_prefix"));
    assert!(diagnostic.contains("<redacted>"));
    assert!(!diagnostic.contains("reader-v1"));
    Ok(())
}

#[test]
fn a_flow_manifest_encodes_no_floating_point_value() -> Result<(), Box<dyn Error>> {
    let plan = golden_plan()?;
    let encoded = std::str::from_utf8(plan.definition_identity().canonical_manifest())?;

    // The reader rejects floats, so a manifest the compiler produced must pass.
    DefinitionManifest::read(plan.definition_identity().canonical_manifest())?;
    assert!(encoded.contains(r#""size":2"#));
    Ok(())
}

struct CompletingTasklet;

impl Tasklet for CompletingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

fn wrapper_job() -> Result<TaskletJob, Box<dyn Error>> {
    Ok(TaskletJob::new(
        JobName::new("daily_import")?,
        TaskletStep::new(StepName::new("import")?, Arc::new(CompletingTasklet)),
        DefinitionRevision::new("wrapper-v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?)
}

#[test]
fn a_format1_wrapper_lowers_without_changing_its_identity() -> Result<(), Box<dyn Error>> {
    let job = wrapper_job()?;
    let untouched = DefinitionIdentity::tasklet(
        &JobName::new("daily_import")?,
        &StepName::new("import")?,
        DefinitionRevision::new("wrapper-v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let plan = job.compiled_plan();

    assert_eq!(plan.manifest_format(), 1);
    assert_eq!(plan.fingerprint(), untouched.manifest_digest());
    assert_eq!(
        plan.definition_identity().canonical_manifest(),
        untouched.canonical_manifest()
    );
    assert_eq!(plan.node_count(), 1);
    assert_eq!(plan.transition_count(), 3);
    assert_eq!(plan.entry().as_str(), "import");
    Ok(())
}

#[test]
fn the_compatibility_plan_routes_every_framework_exit_code() -> Result<(), Box<dyn Error>> {
    let job = wrapper_job()?;
    let plan = job.compiled_plan();
    let entry = plan.entry().clone();

    for (code, expected) in [
        ("COMPLETED", TerminalKind::Complete),
        ("FAILED", TerminalKind::Fail),
        ("STOPPED", TerminalKind::Stop),
    ] {
        assert_eq!(
            plan.select_target(&entry, &oxide_batch::ExitCode::new(code)?)?,
            &FlowTarget::Terminal(expected)
        );
    }
    // An unknown commit is never routed through the graph, so the plan
    // deliberately declares no transition for it.
    assert!(
        plan.select_target(&entry, &oxide_batch::ExitCode::new("UNKNOWN")?)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn moving_a_definition_to_format2_requires_a_direct_upgrade_edge()
-> Result<(), Box<dyn Error>> {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let launcher = JobLauncher::new(&repository, &clock, &ids);
    let (source, stop) = StopSource::new();
    source.request_stop();

    let job = wrapper_job()?;
    launcher.launch(&job, &JobParameters::new(), &stop).await?;

    let format1 = job.compiled_plan().definition_identity().clone();
    let mechanically_equal = oxide_batch::NodeId::new("import")?;
    let format2 = FlowGraph::new(mechanically_equal.clone())
        .with_node(FlowNode::step(StepNode::new(
            mechanically_equal.clone(),
            StepName::new("import")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_sequence(
            mechanically_equal,
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("daily_import")?,
            DefinitionRevision::new("wrapper-v2")?,
        )?;

    assert_ne!(format1.manifest_digest(), format2.fingerprint());

    let instance = {
        let mut unit = repository.begin().await?;
        let instance = unit
            .find_job_instance(&oxide_batch::JobInstanceKey::new(
                JobName::new("daily_import")?,
                &JobParameters::new(),
            ))
            .await?
            .ok_or("the stopped launch must create a job instance")?;
        unit.rollback().await?;
        instance
    };

    let mut unit = repository.begin().await?;
    let rejected = unit
        .create_job_execution_with_definition(instance.id(), format2.definition_identity())
        .await;
    assert!(matches!(
        rejected,
        Err(RepositoryError::IncompatibleDefinition { .. })
    ));
    unit.rollback().await?;

    let upgrade = DefinitionUpgrade::new(
        DefinitionUpgradeKey::new("format1-to-format2")?,
        format1,
        format2.definition_identity().clone(),
        [StepDefinitionUpgrade::new(
            StepName::new("import")?,
            StepName::new("import")?,
        )],
    )?;
    let mut unit = repository.begin().await?;
    unit.register_definition_upgrade(&JobName::new("daily_import")?, &upgrade)
        .await?;
    unit.create_job_execution_with_definition(instance.id(), format2.definition_identity())
        .await?;
    unit.commit().await?;
    Ok(())
}
