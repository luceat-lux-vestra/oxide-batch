//! Named `OPS-CLI-001` conformance scenarios.
//!
//! Each test below is one of the scenarios the M4 design gate requires of the
//! operator CLI. The names are the contract; renaming one breaks the evidence
//! link in the compatibility ledger.

#![allow(clippy::expect_used, clippy::panic)]

mod support;

use oxide_batch_cli::{ExitCategory, MAX_OUTPUT_BYTES, OUTPUT_SCHEMA_VERSION};
use serde_json::json;
use support::{
    FaultyBegin, FaultyExplorer, TestHost, faulty_explorer_services, faulty_repository_services,
    run, run_against, run_expired, seeded_services, services,
};

const CONFIG: &str = "/etc/oxide-batch.json";

// ---------------------------------------------------------------------------
// precedence_resolves_per_value
// ---------------------------------------------------------------------------

#[test]
fn precedence_resolves_per_value() {
    // One value from each source, resolved in the same invocation.
    let mut host = TestHost::new()
        .with_file(
            CONFIG,
            r#"{"config_version":1,"repository":{"pool_size":7},"output":{"page_size":10}}"#,
        )
        .with_env("OXIDE_BATCH_OUTPUT_FORM", "json");
    let (services, _repository) = services();
    let category = run(
        &mut host,
        &services,
        &format!("config show --config {CONFIG} --page-size 25"),
    );
    assert_eq!(category, ExitCategory::Success);

    let envelope = host.envelope();
    let rows = envelope["data"].as_array().expect("data is an array");
    let source_of = |key: &str| -> String {
        rows.iter()
            .find(|row| row["key"] == json!(key))
            .map_or_else(
                || panic!("{key} is missing from config show"),
                |row| row["source"].as_str().unwrap_or_default().to_owned(),
            )
    };
    let value_of = |key: &str| -> String {
        rows.iter()
            .find(|row| row["key"] == json!(key))
            .map_or_else(
                || panic!("{key} is missing from config show"),
                |row| row["value"].as_str().unwrap_or_default().to_owned(),
            )
    };

    // The option wins for the page size even though the file also supplies it.
    assert_eq!(source_of("output.page_size"), "option");
    assert_eq!(value_of("output.page_size"), "25");
    // The environment wins for the output form.
    assert_eq!(source_of("output.form"), "environment");
    assert_eq!(value_of("output.form"), "json");
    // The file still supplies the pool size no source of higher priority named.
    assert_eq!(source_of("repository.pool_size"), "file");
    assert_eq!(value_of("repository.pool_size"), "7");
    // Everything else falls back to the documented default.
    assert_eq!(source_of("client.timeout"), "default");
    assert_eq!(value_of("client.timeout"), "1m");
}

#[test]
fn precedence_resolves_per_value_without_a_configuration_file() {
    let mut host = TestHost::new().with_env("OXIDE_BATCH_OUTPUT_PAGE_SIZE", "12");
    let (services, _repository) = services();
    assert_eq!(
        run(&mut host, &services, "config show --output json"),
        ExitCategory::Success
    );
    let envelope = host.envelope();
    let rows = envelope["data"].as_array().expect("data is an array");
    let row = rows
        .iter()
        .find(|row| row["key"] == json!("output.page_size"))
        .expect("the page size is reported");
    assert_eq!(row["source"], json!("environment"));
    assert_eq!(row["value"], json!("12"));
}

// ---------------------------------------------------------------------------
// unknown_option_or_configuration_key_fails
// ---------------------------------------------------------------------------

#[test]
fn unknown_option_or_configuration_key_fails() {
    let (services, _repository) = services();

    // An unknown option is a usage error and is never ignored.
    let mut host = TestHost::new();
    assert_eq!(
        run(&mut host, &services, "job list --colour red"),
        ExitCategory::Usage
    );
    assert!(host.stderr_text().contains("unknown option"));

    // An unknown subcommand is a usage error.
    let mut host = TestHost::new();
    assert_eq!(
        run(&mut host, &services, "job delete --job orders"),
        ExitCategory::Usage
    );

    // An option a command does not accept is a usage error rather than a
    // silently ignored argument.
    let mut host = TestHost::new();
    assert_eq!(
        run(&mut host, &services, "job list --expected-version 2"),
        ExitCategory::Usage
    );

    // An unknown configuration key fails before any repository is contacted.
    let mut host = TestHost::new().with_file(
        CONFIG,
        r#"{"config_version":1,"output":{"colour":"green"}}"#,
    );
    assert_eq!(
        run(&mut host, &services, &format!("job list --config {CONFIG}")),
        ExitCategory::ConfigurationInvalid
    );
    assert!(host.stderr_text().contains("output.colour"));
    assert!(
        host.stdout_text().is_empty(),
        "a rejected invocation wrote a result"
    );
}

#[test]
fn an_unversioned_or_too_permissive_configuration_file_fails() {
    let (services, _repository) = services();

    let mut host = TestHost::new().with_file(CONFIG, r#"{"output":{"form":"json"}}"#);
    assert_eq!(
        run(&mut host, &services, &format!("job list --config {CONFIG}")),
        ExitCategory::ConfigurationInvalid
    );
    assert!(host.stderr_text().contains("config_version"));

    let mut host = TestHost::new()
        .with_file(CONFIG, r#"{"config_version":1}"#)
        .with_mode(CONFIG, 0o644);
    assert_eq!(
        run(&mut host, &services, &format!("job list --config {CONFIG}")),
        ExitCategory::ConfigurationInvalid
    );
    assert!(host.stderr_text().contains("group or world readable"));
}

// ---------------------------------------------------------------------------
// every_exit_category_is_returned_by_its_named_case
// ---------------------------------------------------------------------------

#[test]
fn every_exit_category_is_returned_by_its_named_case() {
    let mut observed = Vec::new();

    // 0 Success: a bounded read of an empty repository still succeeds.
    let (fixture, _repository) = services();
    let mut host = TestHost::new();
    observed.push((run(&mut host, &fixture, "job list --output json"), 0));

    // 1 Usage: an unknown option.
    let mut host = TestHost::new();
    observed.push((run(&mut host, &fixture, "job list --nope"), 1));

    // 2 Configuration invalid: a page size outside its bound.
    let mut host = TestHost::new();
    observed.push((run(&mut host, &fixture, "job list --page-size 9000"), 2));

    // 3 Guard rejected: launching a job this binary does not register.
    let mut host = TestHost::new();
    observed.push((
        run(
            &mut host,
            &fixture,
            "launch --job orders --actor ops --operation-id op-1",
        ),
        3,
    ));

    // 4 Target not found: an execution that does not exist.
    let mut host = TestHost::new();
    observed.push((
        run(&mut host, &fixture, "execution show --execution 4242"),
        4,
    ));

    // 5 Optimistic conflict: a stop whose expected version is stale loses its
    //   compare-and-swap against a real execution.
    let (seeded_fixture, seeded) = seeded_services("orders");
    let mut host = TestHost::new();
    observed.push((
        run(
            &mut host,
            &seeded_fixture,
            &format!(
                "execution stop --execution {} --expected-version {} --actor ops \
                 --operation-id op-2",
                seeded.execution_id,
                seeded.version + 7
            ),
        ),
        5,
    ));

    // 6 Outcome unknown: the repository cannot report whether it committed.
    let unknown = faulty_repository_services(FaultyBegin::OutcomeUnknown);
    let mut host = TestHost::new();
    observed.push((
        run_against(
            &mut host,
            &unknown,
            "retention hold --instance 1 --actor ops --reason LEGAL --operation-id op-4 --yes",
        ),
        6,
    ));

    // 7 Repository unavailable: the read port cannot answer.
    let unavailable = faulty_explorer_services(FaultyExplorer::Unavailable);
    let mut host = TestHost::new();
    observed.push((run_against(&mut host, &unavailable, "job list"), 7));

    // 8 Confirmation required: a destructive command without --yes and
    //   without an interactive terminal.
    let mut host = TestHost::new();
    observed.push((
        run(
            &mut host,
            &fixture,
            "retention hold --instance 1 --actor ops --reason LEGAL --operation-id op-5",
        ),
        8,
    ));

    // 9 Deadline exceeded: the client deadline elapses while the query stalls.
    let stalled = faulty_explorer_services(FaultyExplorer::Stalled);
    let mut host = TestHost::new();
    observed.push((run_expired(&mut host, &stalled, "job list"), 9));

    // 10 Output failure: standard output refuses the result.
    let mut host = TestHost::new().with_stdout_capacity(0);
    observed.push((run(&mut host, &fixture, "job list --output json"), 10));

    // 70 Internal: an injected identifier source failed, which is a defect
    //    rather than an operator error.
    let broken = faulty_repository_services(FaultyBegin::Identifier);
    let mut host = TestHost::new();
    observed.push((
        run_against(
            &mut host,
            &broken,
            "retention hold --instance 1 --actor ops --reason LEGAL --operation-id op-6 --yes",
        ),
        70,
    ));

    for (category, expected) in &observed {
        assert_eq!(
            category.code(),
            *expected,
            "{category} returned code {} instead of {expected}",
            category.code()
        );
    }

    // Every published category is covered by a named case above.
    let mut covered: Vec<u8> = observed.iter().map(|(_, code)| *code).collect();
    covered.sort_unstable();
    covered.dedup();
    let mut published: Vec<u8> = ExitCategory::all().iter().map(|c| c.code()).collect();
    published.sort_unstable();
    assert_eq!(
        covered, published,
        "a published exit category has no named case"
    );
}

// ---------------------------------------------------------------------------
// destructive_command_without_yes_exits_confirmation_required
// ---------------------------------------------------------------------------

#[test]
fn destructive_command_without_yes_exits_confirmation_required() {
    let (services, _repository) = services();

    // Non-interactive and without --yes: refused, and nothing is written to
    // standard output because no effect was attempted.
    let mut host = TestHost::new();
    let category = run(
        &mut host,
        &services,
        "retention hold --instance 1 --actor ops --reason LEGAL --operation-id op-1",
    );
    assert_eq!(category, ExitCategory::ConfirmationRequired);
    assert!(host.stderr_text().contains("requires --yes"));
    assert!(host.stdout_text().is_empty());

    // Interactive and explicitly declined: still refused.
    let mut host = TestHost::new().interactive("no");
    let category = run(
        &mut host,
        &services,
        "retention hold --instance 1 --actor ops --reason LEGAL --operation-id op-2",
    );
    assert_eq!(category, ExitCategory::ConfirmationRequired);
    assert!(host.stderr_text().contains("CONFIRMATION_DECLINED"));

    // Interactive with no response at all is never taken as confirmation.
    let mut host = TestHost::new().interactive_silent();
    let category = run(
        &mut host,
        &services,
        "retention hold --instance 1 --actor ops --reason LEGAL --operation-id op-3",
    );
    assert_eq!(category, ExitCategory::ConfirmationRequired);

    // The prompt names the target, the action class, and the operation id.
    let mut host = TestHost::new().interactive("no");
    let _ = run(
        &mut host,
        &services,
        "retention hold --instance 7 --actor ops --reason LEGAL --operation-id op-4",
    );
    let prompt = host.stderr_text();
    assert!(prompt.contains("retention hold"));
    assert!(prompt.contains("DESTRUCTIVE"));
    assert!(prompt.contains("instance=7"));
    assert!(prompt.contains("op-4"));
}

#[test]
fn a_mutating_command_requires_an_explicit_operation_id_when_not_interactive() {
    let (services, _repository) = services();
    let mut host = TestHost::new();
    let category = run(
        &mut host,
        &services,
        "execution stop --execution 1 --expected-version 0 --actor ops",
    );
    assert_eq!(category, ExitCategory::Usage);
    assert!(host.stderr_text().contains("OPERATION_ID_REQUIRED"));
}

// ---------------------------------------------------------------------------
// dry_run_makes_no_durable_change
// ---------------------------------------------------------------------------

#[test]
fn dry_run_makes_no_durable_change() {
    let (services, _repository) = services();

    // A dry-run launch of an unregistered job still reports the guard, and a
    // dry-run of a registered job reports the request without applying it.
    let mut host = TestHost::new();
    let category = run(
        &mut host,
        &services,
        "launch --job orders --actor ops --operation-id op-1 --dry-run --output json",
    );
    // Without a registered definition the guard is reported before any effect.
    assert_eq!(category, ExitCategory::GuardRejected);
    assert!(host.stdout_text().contains("JOB_NOT_REGISTERED"));

    // The audit trail records nothing, because nothing was applied.
    let mut host = TestHost::new();
    assert_eq!(
        run(
            &mut host,
            &services,
            "execution history --execution 1 --record operator --output json"
        ),
        ExitCategory::Success
    );
    assert_eq!(host.envelope()["data"], json!([]));

    // A dry-run retention apply validates the plan and deletes nothing.
    let mut host = TestHost::new();
    let category = run(
        &mut host,
        &services,
        "retention apply --job orders --older-than 30d --plan-digest \
         0000000000000000000000000000000000000000000000000000000000000000 \
         --actor ops --reason CLEANUP --operation-id op-2 --yes --dry-run --output json",
    );
    // An empty repository produces an empty plan whose digest is not the
    // supplied one, so the stale-plan guard refuses before any deletion.
    assert_eq!(category, ExitCategory::OptimisticConflict);
    let envelope = host.envelope();
    assert_eq!(envelope["outcome"], json!("conflict"));
    assert!(
        envelope["diagnostics"][0]["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("nothing was deleted")
    );
}

#[test]
fn dry_run_reports_the_request_digest_without_applying_it() {
    use oxide_batch::{
        ComponentRevision, DefinitionIdentity, DefinitionRevision, JobName, StepName,
    };
    use oxide_batch_cli::DefinitionCatalog;

    let job = JobName::new("orders").expect("the job name is valid");
    let step = StepName::new("only").expect("the step name is valid");
    let revision = DefinitionRevision::new("r1").expect("the revision is valid");
    let component = ComponentRevision::new("c1").expect("the component revision is valid");
    let identity = DefinitionIdentity::tasklet(&job, &step, revision, &component)
        .expect("the manifest encodes");
    let catalog = DefinitionCatalog::new()
        .with(identity)
        .expect("the registration succeeds");

    let (services, _repository) = services();
    let mut host = TestHost::new();
    let category = support::run_with_catalog(
        &mut host,
        &services,
        &catalog,
        "launch --job orders --actor ops --operation-id op-1 --dry-run --output json",
    );
    assert_eq!(category, ExitCategory::Success);
    let envelope = host.envelope();
    assert_eq!(envelope["data"]["dry_run"], json!(true));
    assert_eq!(envelope["data"]["applied"], json!(false));
    assert_eq!(envelope["data"]["action"], json!("LAUNCH"));
    assert!(
        envelope["data"]["request_digest"]
            .as_str()
            .is_some_and(|digest| digest.len() == 64)
    );

    // Nothing became durable: the job still has no instance.
    let mut host = TestHost::new();
    assert_eq!(
        run(
            &mut host,
            &services,
            "instance list --job orders --output json"
        ),
        ExitCategory::Success
    );
    assert_eq!(host.envelope()["data"], json!([]));
}

// ---------------------------------------------------------------------------
// broken_stdout_stops_output_and_repeats_no_mutation
// ---------------------------------------------------------------------------

#[test]
fn broken_stdout_stops_output_and_repeats_no_mutation() {
    let (services, _repository) = services();

    // A read whose output cannot be written reports the output category and
    // attempts exactly one write.
    let mut host = TestHost::new().with_stdout_capacity(0);
    let category = run(&mut host, &services, "job list --output json");
    assert_eq!(category, ExitCategory::OutputFailure);
    assert_eq!(host.writes, 1, "output was retried after a closed pipe");
    assert!(host.stdout_text().is_empty());
}

#[test]
fn a_mutation_whose_output_fails_is_applied_exactly_once() {
    let (services, seeded) = seeded_services("orders");

    // Place a hold whose result cannot be displayed.
    let mut host = TestHost::new().with_stdout_capacity(0);
    let line = format!(
        "retention hold --instance {} --actor ops --reason LEGAL --operation-id op-1 --yes",
        seeded.instance_id
    );
    let category = run(&mut host, &services, &line);
    assert_eq!(category, ExitCategory::OutputFailure);
    assert_eq!(host.writes, 1, "the result was written more than once");

    // Replaying the same operation identifier returns the recorded outcome
    // rather than applying a second hold.
    let mut host = TestHost::new();
    let category = run(&mut host, &services, &format!("{line} --output json"));
    assert_eq!(category, ExitCategory::Success);
    assert_eq!(host.envelope()["data"]["outcome"], json!("REPLAYED"));
}

// ---------------------------------------------------------------------------
// json_output_matches_the_published_schema_and_redaction_rules
// ---------------------------------------------------------------------------

#[test]
fn json_output_matches_the_published_schema_and_redaction_rules() {
    let (services, _repository) = services();
    let mut host = TestHost::new();
    assert_eq!(
        run(
            &mut host,
            &services,
            "job list --output json --page-size 25"
        ),
        ExitCategory::Success
    );

    let envelope = host.envelope();
    // Exactly one object per invocation, carrying every published field.
    assert_eq!(host.stdout_text().trim_end().lines().count(), 1);
    assert_eq!(envelope["schema_version"], json!(OUTPUT_SCHEMA_VERSION));
    assert_eq!(envelope["command"], json!("job list"));
    assert_eq!(envelope["outcome"], json!("success"));
    assert!(envelope["data"].is_array());
    assert_eq!(envelope["page"]["page_size"], json!(25));
    assert_eq!(envelope["page"]["returned"], json!(0));
    assert!(envelope["page"].get("next_cursor").is_some());
    assert!(envelope["diagnostics"].is_array());
    assert_eq!(envelope["truncated"], json!(false));

    let published = [
        "schema_version",
        "command",
        "outcome",
        "data",
        "page",
        "diagnostics",
        "truncated",
    ];
    let object = envelope.as_object().expect("the envelope is an object");
    for key in object.keys() {
        assert!(
            published.contains(&key.as_str()),
            "the envelope carries an unpublished field {key}"
        );
    }
    assert!(host.stdout_text().len() <= MAX_OUTPUT_BYTES);
}

#[test]
fn json_output_redacts_every_secret_bearing_configuration_value() {
    let (services, _repository) = services();
    let secret = "postgres://batch:hunter2@db.internal:5432/oxide";
    let mut host = TestHost::new()
        .with_env("OXIDE_BATCH_REPOSITORY_URL", secret)
        .with_env(
            "OXIDE_BATCH_REPOSITORY_CA_CERTIFICATE",
            "-----BEGIN CERTIFICATE-----",
        );
    assert_eq!(
        run(&mut host, &services, "config show --output json"),
        ExitCategory::Success
    );

    let text = host.stdout_text();
    assert!(!text.contains("hunter2"), "a password reached the output");
    assert!(
        !text.contains("db.internal"),
        "a host name reached the output"
    );
    assert!(
        !text.contains("BEGIN CERTIFICATE"),
        "a certificate reached the output"
    );

    let envelope = host.envelope();
    let rows = envelope["data"].as_array().expect("data is an array");
    for key in ["repository.url", "repository.ca_certificate"] {
        let row = rows
            .iter()
            .find(|row| row["key"] == json!(key))
            .unwrap_or_else(|| panic!("{key} is missing"));
        assert_eq!(row["redacted"], json!(true));
        assert_eq!(row["value"], json!("<redacted>"));
        assert_eq!(row["source"], json!("environment"));
    }
}

#[test]
fn the_human_form_also_redacts_every_secret_bearing_value() {
    let (services, _repository) = services();
    let mut host = TestHost::new().with_env(
        "OXIDE_BATCH_REPOSITORY_URL",
        "postgres://batch:hunter2@db.internal/oxide",
    );
    assert_eq!(
        run(&mut host, &services, "config show --output human"),
        ExitCategory::Success
    );
    let text = host.stdout_text();
    assert!(!text.contains("hunter2"));
    assert!(!text.contains("db.internal"));
    assert!(text.contains("<redacted>"));
}

#[test]
fn a_rejected_action_reports_its_outcome_without_driver_text() {
    let unavailable = faulty_explorer_services(FaultyExplorer::Unavailable);
    let mut host = TestHost::new();
    let category = run_against(&mut host, &unavailable, "job list --output json");
    assert_eq!(category, ExitCategory::RepositoryUnavailable);
    let envelope = host.envelope();
    assert_eq!(envelope["outcome"], json!("error"));
    assert_eq!(
        envelope["diagnostics"][0]["code"],
        json!("REPOSITORY_UNAVAILABLE")
    );
    let detail = envelope["diagnostics"][0]["detail"]
        .as_str()
        .expect("the diagnostic has a detail");
    assert!(!detail.to_lowercase().contains("select"));
    assert!(!detail.to_lowercase().contains("sqlx"));
}
