//! The process boundary one invocation reads and writes.
//!
//! Every environment variable, file read, byte written, confirmation prompt,
//! and generated operation identifier passes through [`Host`]. Tests supply a
//! deterministic host, so broken output, refused confirmation, file
//! permissions, and per-value precedence are ordinary assertions rather than
//! process-level fixtures.

use std::io::{self, IsTerminal, Read, Write};
use std::path::Path;

/// The process services one invocation requires.
pub trait Host {
    /// Reads one environment variable.
    ///
    /// A variable that is present but empty is treated as absent, so an empty
    /// value never shadows a configuration file.
    fn env(&self, key: &str) -> Option<String>;

    /// Reads one file.
    ///
    /// # Errors
    ///
    /// Returns the underlying input/output failure.
    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Returns the Unix permission bits of a file, when the platform has them.
    ///
    /// # Errors
    ///
    /// Returns the underlying metadata failure.
    fn file_mode(&self, path: &Path) -> io::Result<Option<u32>>;

    /// Writes to standard output.
    ///
    /// # Errors
    ///
    /// Returns the underlying write failure, including a closed pipe.
    fn write_stdout(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Flushes standard output.
    ///
    /// # Errors
    ///
    /// Returns the underlying flush failure.
    fn flush_stdout(&mut self) -> io::Result<()>;

    /// Writes a redacted diagnostic to standard error.
    ///
    /// A diagnostic write failure never changes the exit category, because the
    /// command's durable effect does not depend on it.
    fn write_stderr(&mut self, bytes: &[u8]);

    /// Returns whether standard input is an interactive terminal.
    fn is_stdin_interactive(&self) -> bool;

    /// Returns whether standard output is a terminal that can carry styling.
    fn is_stdout_terminal(&self) -> bool;

    /// Reads one confirmation response from standard input.
    ///
    /// Returns `None` when input ended without a response. An empty response
    /// is never confirmation.
    ///
    /// # Errors
    ///
    /// Returns the underlying read failure.
    fn read_confirmation(&mut self) -> io::Result<Option<String>>;

    /// Returns a fresh operation identifier for an interactive mutation.
    ///
    /// The value is printed before the effect is attempted so that an operator
    /// can replay the request after an ambiguous outcome.
    fn new_operation_id(&mut self) -> String;
}

/// The real process host.
#[derive(Debug)]
pub struct ProcessHost {
    stdout: io::Stdout,
    stderr: io::Stderr,
    counter: u64,
}

impl ProcessHost {
    /// Binds the current process streams.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stdout: io::stdout(),
            stderr: io::stderr(),
            counter: 0,
        }
    }
}

impl Default for ProcessHost {
    fn default() -> Self {
        Self::new()
    }
}

impl Host for ProcessHost {
    fn env(&self, key: &str) -> Option<String> {
        std::env::var(key).ok().filter(|value| !value.is_empty())
    }

    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn file_mode(&self, path: &Path) -> io::Result<Option<u32>> {
        let metadata = std::fs::metadata(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            Ok(Some(metadata.permissions().mode()))
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Ok(None)
        }
    }

    fn write_stdout(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stdout.write_all(bytes)
    }

    fn flush_stdout(&mut self) -> io::Result<()> {
        self.stdout.flush()
    }

    fn write_stderr(&mut self, bytes: &[u8]) {
        let _ = self.stderr.write_all(bytes);
        let _ = self.stderr.flush();
    }

    fn is_stdin_interactive(&self) -> bool {
        io::stdin().is_terminal()
    }

    fn is_stdout_terminal(&self) -> bool {
        self.stdout.is_terminal()
    }

    fn read_confirmation(&mut self) -> io::Result<Option<String>> {
        let mut buffer = String::new();
        let mut handle = io::stdin().lock();
        let mut byte = [0_u8; 1];
        loop {
            match handle.read(&mut byte)? {
                0 => break,
                _ if byte[0] == b'\n' => break,
                _ => buffer.push(char::from(byte[0])),
            }
            if buffer.len() > MAX_CONFIRMATION_BYTES {
                break;
            }
        }
        if buffer.is_empty() {
            return Ok(None);
        }
        Ok(Some(buffer))
    }

    fn new_operation_id(&mut self) -> String {
        // The identifier only needs to be unique for one operator's replay of
        // one request, so process identity plus a monotonic counter is
        // sufficient and adds no dependency.
        self.counter += 1;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |value| value.as_nanos());
        format!("cli-{}-{nanos}-{}", std::process::id(), self.counter)
    }
}

/// The largest confirmation response the CLI reads.
const MAX_CONFIRMATION_BYTES: usize = 64;

#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::io;
    use std::path::{Path, PathBuf};

    use super::Host;

    /// A deterministic in-memory host.
    #[derive(Debug, Default)]
    pub(crate) struct TestHost {
        pub(crate) env: BTreeMap<String, String>,
        pub(crate) files: BTreeMap<PathBuf, Vec<u8>>,
        pub(crate) modes: BTreeMap<PathBuf, u32>,
        pub(crate) stdout: Vec<u8>,
        pub(crate) stderr: Vec<u8>,
        pub(crate) stdin_interactive: bool,
        pub(crate) stdout_terminal: bool,
        pub(crate) confirmation: Option<String>,
        /// Number of bytes standard output accepts before it fails.
        pub(crate) stdout_capacity: Option<usize>,
        pub(crate) operation_ids: u64,
    }

    impl TestHost {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn with_env(mut self, key: &str, value: &str) -> Self {
            self.env.insert(key.to_owned(), value.to_owned());
            self
        }

        pub(crate) fn with_file(mut self, path: &str, contents: &str) -> Self {
            self.files
                .insert(PathBuf::from(path), contents.as_bytes().to_vec());
            self.modes.insert(PathBuf::from(path), 0o600);
            self
        }

        pub(crate) fn with_mode(mut self, path: &str, mode: u32) -> Self {
            self.modes.insert(PathBuf::from(path), mode);
            self
        }

        pub(crate) fn with_stdout_capacity(mut self, bytes: usize) -> Self {
            self.stdout_capacity = Some(bytes);
            self
        }

        pub(crate) fn stdout_text(&self) -> String {
            String::from_utf8_lossy(&self.stdout).into_owned()
        }
    }

    impl Host for TestHost {
        fn env(&self, key: &str) -> Option<String> {
            self.env.get(key).cloned().filter(|value| !value.is_empty())
        }

        fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
            self.files.get(path).cloned().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "the test host has no such file")
            })
        }

        fn file_mode(&self, path: &Path) -> io::Result<Option<u32>> {
            if !self.files.contains_key(path) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "the test host has no such file",
                ));
            }
            Ok(self.modes.get(path).copied())
        }

        fn write_stdout(&mut self, bytes: &[u8]) -> io::Result<()> {
            if let Some(capacity) = self.stdout_capacity
                && self.stdout.len() + bytes.len() > capacity
            {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe"));
            }
            self.stdout.extend_from_slice(bytes);
            Ok(())
        }

        fn flush_stdout(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_stderr(&mut self, bytes: &[u8]) {
            self.stderr.extend_from_slice(bytes);
        }

        fn is_stdin_interactive(&self) -> bool {
            self.stdin_interactive
        }

        fn is_stdout_terminal(&self) -> bool {
            self.stdout_terminal
        }

        fn read_confirmation(&mut self) -> io::Result<Option<String>> {
            Ok(self.confirmation.take())
        }

        fn new_operation_id(&mut self) -> String {
            self.operation_ids += 1;
            format!("test-operation-{}", self.operation_ids)
        }
    }
}
