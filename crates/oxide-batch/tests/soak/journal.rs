//! Per-cycle evidence, written out of the process that is being measured.
//!
//! This exists because of a confound that is easy to miss and, once seen,
//! disqualifies the obvious implementation. The campaign measures the resident
//! memory of the process it runs in, and the obvious way to build its report is
//! to collect each cycle's evidence into a vector and render the whole thing at
//! the end. That vector is retained memory that grows once per cycle, inside
//! the very process whose growth is the result — around thirteen kilobytes a
//! cycle in the first implementation of this report, which is a straight line
//! through the measured window and had nothing to do with the framework.
//!
//! Loosening the memory rule until the campaign's own bookkeeping fit under it
//! would have been the wrong repair twice over: it would weaken the rule for
//! real accumulation as well, and it would leave the report's number partly
//! measuring the report. So the evidence leaves the process as it is produced,
//! one JSON document per line, and is read back after the last sample has been
//! taken. What stays resident per cycle is a handful of integers per declared
//! metric, in vectors reserved to their final length before the measured window
//! opens.
//!
//! The journal is written under `target/`, is named for the process that owns
//! it, and is removed when the report finishes.

use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::process;

use serde_json::Value;

use super::workspace_root;

/// One append-only line-delimited record of the run.
pub struct Journal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Journal {
    /// Opens an empty journal for one kind of record.
    ///
    /// # Errors
    ///
    /// Returns the filesystem failure that prevented the journal.
    pub fn open(kind: &str) -> Result<Self, Box<dyn Error>> {
        let directory = workspace_root().join("target").join("m5-soak-journal");
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{kind}-{}.jsonl", process::id()));
        let writer = BufWriter::new(File::create(&path)?);
        Ok(Self { path, writer })
    }

    /// Appends one record.
    ///
    /// # Errors
    ///
    /// Returns the failure when the record cannot be rendered or written.
    pub fn append(&mut self, value: &Value) -> Result<(), Box<dyn Error>> {
        // Rendered to a line rather than held, which is the whole point: the
        // allocation is transient and the retained bytes leave the process.
        writeln!(self.writer, "{}", serde_json::to_string(value)?)?;
        Ok(())
    }

    /// Reads every record back and removes the journal.
    ///
    /// Called after the last sample is taken, so the memory this allocates is
    /// outside the measured window.
    ///
    /// # Errors
    ///
    /// Returns the failure when the journal cannot be flushed, read, or parsed.
    pub fn take(mut self) -> Result<Vec<Value>, Box<dyn Error>> {
        self.writer.flush()?;
        let file = File::open(&self.path)?;
        let mut records = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records.push(serde_json::from_str(&line)?);
        }
        let _ = fs::remove_file(&self.path);
        Ok(records)
    }
}
