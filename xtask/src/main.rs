//! Repository development tasks.

use std::env;
use std::ffi::OsStr;
use std::process::{Command, ExitCode};

struct Task<'a> {
    label: &'a str,
    program: &'a str,
    args: &'a [&'a str],
    rustdoc_warnings: bool,
}

fn main() -> ExitCode {
    let mut args = env::args();
    let _program = args.next();

    match args.next().as_deref() {
        Some("check") => run_all(&[
            Task {
                label: "format",
                program: "cargo",
                args: &["fmt", "--all", "--", "--check"],
                rustdoc_warnings: false,
            },
            Task {
                label: "clippy",
                program: "cargo",
                args: &[
                    "clippy",
                    "--workspace",
                    "--all-targets",
                    "--all-features",
                    "--",
                    "-D",
                    "warnings",
                ],
                rustdoc_warnings: false,
            },
            Task {
                label: "tests",
                program: "cargo",
                args: &["test", "--workspace", "--all-features"],
                rustdoc_warnings: false,
            },
            Task {
                label: "documentation",
                program: "cargo",
                args: &["doc", "--workspace", "--all-features", "--no-deps"],
                rustdoc_warnings: true,
            },
        ]),
        Some("doctor") => run_all(&[
            Task {
                label: "Rust compiler",
                program: "rustc",
                args: &["--version"],
                rustdoc_warnings: false,
            },
            Task {
                label: "Cargo",
                program: "cargo",
                args: &["--version"],
                rustdoc_warnings: false,
            },
            Task {
                label: "Git",
                program: "git",
                args: &["--version"],
                rustdoc_warnings: false,
            },
        ]),
        Some("package") => run_all(&[
            Task {
                label: "package contents",
                program: "cargo",
                args: &["package", "--package", "oxide-batch", "--list"],
                rustdoc_warnings: false,
            },
            Task {
                label: "publish dry run",
                program: "cargo",
                args: &[
                    "publish",
                    "--package",
                    "oxide-batch",
                    "--locked",
                    "--dry-run",
                ],
                rustdoc_warnings: false,
            },
        ]),
        Some(command) => {
            eprintln!("unknown xtask command: {command}");
            usage();
            ExitCode::FAILURE
        }
        None => {
            usage();
            ExitCode::FAILURE
        }
    }
}

fn run_all(tasks: &[Task<'_>]) -> ExitCode {
    for task in tasks {
        eprintln!("==> {}", task.label);

        let mut command = Command::new(task.program);
        command.args(task.args);

        if task.rustdoc_warnings {
            command.env("RUSTDOCFLAGS", "-D warnings");
        }

        match command.status() {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("{} failed with {status}", task.label);
                return ExitCode::FAILURE;
            }
            Err(error) => {
                eprintln!("could not run {}: {error}", display_command(task));
                return ExitCode::FAILURE;
            }
        }
    }

    ExitCode::SUCCESS
}

fn display_command(task: &Task<'_>) -> String {
    std::iter::once(OsStr::new(task.program))
        .chain(task.args.iter().map(OsStr::new))
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

fn usage() {
    eprintln!(
        "usage: cargo xtask <command>\n\n\
         commands:\n\
           check    run formatting, Clippy, tests, and rustdoc\n\
           doctor   show required local tool versions\n\
           package  inspect and dry-run the public facade package"
    );
}
