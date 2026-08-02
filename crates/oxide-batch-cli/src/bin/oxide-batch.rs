//! The shipped `oxide-batch` operator binary.
//!
//! This binary registers no job definition, so `launch` and
//! `execution restart` report a deterministic `JOB_NOT_REGISTERED` rejection.
//! Every other command is served against the configured repository. An
//! application that wants to launch or restart from a command line embeds the
//! `oxide-batch-cli` library and supplies its own
//! [`oxide_batch_cli::DefinitionCatalog`].

use std::process::ExitCode;

use oxide_batch_cli::{DefinitionCatalog, ExitCategory, Host, ProcessHost};

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let category = run(&arguments);
    ExitCode::from(category.code())
}

/// Runs one invocation and returns its exit category.
fn run(arguments: &[String]) -> ExitCategory {
    let mut host = ProcessHost::new();
    let mut plan = match oxide_batch_cli::prepare(&mut host, arguments) {
        Ok(plan) => plan,
        Err(category) => return category,
    };
    if let Some(category) = oxide_batch_cli::local(&mut host, &plan) {
        return category;
    }
    // The runtime is created only once a command actually needs a connection,
    // so a usage or configuration error never starts one.
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        host.write_stderr(b"INTERNAL: the async runtime could not be created\n");
        return ExitCategory::Internal;
    };
    let catalog = DefinitionCatalog::new();
    runtime.block_on(async {
        let services = match oxide_batch_cli::connect(plan.config()).await {
            Ok(services) => services,
            Err(failure) => {
                host.write_stderr(
                    format!(
                        "{}: {}: {}\n",
                        failure.category(),
                        failure.diagnostic().code,
                        failure.diagnostic().detail
                    )
                    .as_bytes(),
                );
                return failure.category();
            }
        };
        let deadline = tokio::time::sleep(plan.timeout());
        oxide_batch_cli::dispatch(&mut host, &mut plan, &services, &catalog, deadline).await
    })
}
