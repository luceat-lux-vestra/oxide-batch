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

const HELP: &str = "oxide-batch - guarded repository operator\n\
\n\
USAGE:\n\
  oxide-batch <noun> <verb> [options]\n\
  oxide-batch launch [options]\n\
\n\
NOUNS:\n\
  job  instance  execution  retention  config  schema  diagnostics\n\
\n\
BOUNDARY:\n\
  This binary is not a standalone job-definition loader. It registers no Rust\n\
  job definitions, so launch and execution restart require an application that\n\
  embeds oxide-batch-cli and supplies a DefinitionCatalog.\n\
\n\
See the Operator CLI Reference for the closed command and option grammar.\n";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let category = run(&arguments);
    ExitCode::from(category.code())
}

/// Runs one invocation and returns its exit category.
fn run(arguments: &[String]) -> ExitCategory {
    let mut host = ProcessHost::new();
    if matches!(arguments, [argument] if argument == "--help" || argument == "-h") {
        return if host.write_stdout(HELP.as_bytes()).is_ok() && host.flush_stdout().is_ok() {
            ExitCategory::Success
        } else {
            ExitCategory::OutputFailure
        };
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_is_a_successful_local_command() {
        assert_eq!(run(&["--help".to_owned()]), ExitCategory::Success);
    }
}
