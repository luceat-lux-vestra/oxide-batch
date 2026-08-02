//! Bounded human and machine output.
//!
//! Both forms are rendered from one already-redacted projection, so a redaction
//! rule cannot hold in the machine form while the human form leaks. Output is
//! written only after a mutating command's durable effect is committed, so a
//! display failure can never lose an effect.

use serde_json::{Map, Value, json};

use crate::args::OutputForm;
use crate::command::Command;
use crate::exit::{ExitCategory, Outcome};
use crate::host::Host;

/// The integer CLI output schema version.
pub const OUTPUT_SCHEMA_VERSION: u64 = 1;

/// Largest encoded response written for one invocation.
pub const MAX_OUTPUT_BYTES: usize = 256 * 1024;

/// Pagination fields of a paginated command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageInfo {
    /// The bound the traversal requested.
    pub page_size: u16,
    /// The number of rows this page returned.
    pub returned: usize,
    /// The opaque token that continues the traversal.
    pub next_cursor: Option<String>,
}

/// One bounded, redacted diagnostic record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    /// Stable machine code.
    pub code: String,
    /// Safe-to-display detail. Never a secret, SQL, or user error text.
    pub detail: String,
}

impl Diagnostic {
    /// Builds one diagnostic record.
    #[must_use]
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

/// The complete result of one invocation.
#[derive(Clone, Debug)]
pub struct Response {
    command: Command,
    category: ExitCategory,
    data: Value,
    page: Option<PageInfo>,
    diagnostics: Vec<Diagnostic>,
}

impl Response {
    /// Builds a successful response carrying one redacted projection.
    #[must_use]
    pub const fn success(command: Command, data: Value) -> Self {
        Self {
            command,
            category: ExitCategory::Success,
            data,
            page: None,
            diagnostics: Vec::new(),
        }
    }

    /// Builds a response that reports a non-success category.
    #[must_use]
    pub const fn failed(command: Command, category: ExitCategory, data: Value) -> Self {
        Self {
            command,
            category,
            data,
            page: None,
            diagnostics: Vec::new(),
        }
    }

    /// Attaches the pagination fields of a paginated command.
    #[must_use]
    pub fn with_page(mut self, page: PageInfo) -> Self {
        self.page = Some(page);
        self
    }

    /// Attaches one bounded diagnostic record.
    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: Diagnostic) -> Self {
        self.diagnostics.push(diagnostic);
        self
    }

    /// Returns the exit category this response reports.
    #[must_use]
    pub const fn category(&self) -> ExitCategory {
        self.category
    }

    /// Returns the envelope outcome this response reports.
    #[must_use]
    pub const fn outcome(&self) -> Outcome {
        self.category.outcome()
    }
}

/// A failure to write the result.
///
/// The operation identifier lets the caller re-read or replay the request, so
/// the CLI never repeats a mutating call to recover from a display failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputFailure;

/// Renders responses in the selected form.
#[derive(Clone, Copy, Debug)]
pub struct Writer {
    form: OutputForm,
    color: bool,
}

impl Writer {
    /// Binds one output form and styling decision.
    #[must_use]
    pub const fn new(form: OutputForm, color: bool) -> Self {
        Self { form, color }
    }

    /// Returns the selected output form.
    #[must_use]
    pub const fn form(&self) -> OutputForm {
        self.form
    }

    /// Writes one response.
    ///
    /// Output stops at the first write failure and performs no further write,
    /// so a closed pipe produces one bounded attempt rather than a loop.
    ///
    /// # Errors
    ///
    /// Returns [`OutputFailure`] when standard output cannot be written.
    pub fn emit<H: Host>(&self, host: &mut H, response: &Response) -> Result<(), OutputFailure> {
        let rendered = match self.form {
            OutputForm::Json => fit(&response.data, |data, truncated| {
                render_json(response, data, truncated)
            }),
            OutputForm::Human => fit(&response.data, |data, truncated| {
                self.render_human(response, data, truncated)
            }),
        };
        // Every write failure is the same observable event: the result could
        // not be delivered. The underlying error is deliberately not inspected,
        // because its message may name a path or a device.
        host.write_stdout(rendered.as_bytes())
            .and_then(|()| host.flush_stdout())
            .map_err(|_| OutputFailure)
    }

    /// Renders the unversioned presentation form from already-bounded data.
    fn render_human(self, response: &Response, data: &Value, truncated: bool) -> String {
        use std::fmt::Write as _;

        let mut text = String::new();
        if !matches!(response.outcome(), Outcome::Success) {
            self.heading(response.category.as_str(), &mut text);
            text.push('\n');
        }
        render_value(data, 0, self, &mut text);
        if let Some(page) = &response.page {
            self.heading("page", &mut text);
            text.push('\n');
            // Writing into a `String` cannot fail, so the formatting result
            // carries no information a caller could act on.
            let _ = writeln!(
                text,
                "  page_size: {}\n  returned: {}",
                page.page_size, page.returned
            );
            match &page.next_cursor {
                Some(cursor) => {
                    let _ = writeln!(text, "  next_cursor: {cursor}");
                }
                None => text.push_str("  next_cursor: -\n"),
            }
        }
        for diagnostic in &response.diagnostics {
            let _ = writeln!(text, "{}: {}", diagnostic.code, diagnostic.detail);
        }
        if truncated {
            text.push_str("truncated: true\n");
        }
        text
    }

    /// Appends a heading, styled when styling is enabled.
    fn heading(self, text: &str, target: &mut String) {
        if self.color {
            target.push_str("\u{1b}[1m");
            target.push_str(text);
            target.push_str("\u{1b}[0m");
        } else {
            target.push_str(text);
        }
    }
}

/// Renders the versioned machine envelope from already-bounded data.
fn render_json(response: &Response, data: &Value, truncated: bool) -> String {
    let mut envelope = Map::new();
    envelope.insert("schema_version".to_owned(), json!(OUTPUT_SCHEMA_VERSION));
    envelope.insert("command".to_owned(), json!(response.command.as_str()));
    envelope.insert("outcome".to_owned(), json!(response.outcome().as_str()));
    envelope.insert("data".to_owned(), data.clone());
    if let Some(page) = &response.page {
        envelope.insert(
            "page".to_owned(),
            json!({
                "page_size": page.page_size,
                "returned": page.returned,
                "next_cursor": page.next_cursor,
            }),
        );
    }
    envelope.insert(
        "diagnostics".to_owned(),
        Value::Array(
            response
                .diagnostics
                .iter()
                .map(|diagnostic| json!({ "code": diagnostic.code, "detail": diagnostic.detail }))
                .collect(),
        ),
    );
    envelope.insert("truncated".to_owned(), json!(truncated));
    let mut encoded = Value::Object(envelope).to_string();
    encoded.push('\n');
    encoded
}

/// Renders one JSON value as indented human text.
fn render_value(value: &Value, depth: usize, writer: Writer, text: &mut String) {
    use std::fmt::Write as _;

    let indent = "  ".repeat(depth);
    match value {
        Value::Object(entries) => {
            for (key, entry) in entries {
                match entry {
                    Value::Object(_) | Value::Array(_) => {
                        text.push_str(&indent);
                        writer.heading(key, text);
                        text.push('\n');
                        render_value(entry, depth + 1, writer, text);
                    }
                    _ => {
                        let _ = writeln!(text, "{indent}{key}: {}", scalar(entry));
                    }
                }
            }
        }
        Value::Array(rows) => {
            if rows.is_empty() {
                let _ = writeln!(text, "{indent}-");
            }
            for row in rows {
                match row {
                    Value::Object(_) | Value::Array(_) => {
                        let _ = writeln!(text, "{indent}-");
                        render_value(row, depth + 1, writer, text);
                    }
                    _ => {
                        let _ = writeln!(text, "{indent}- {}", scalar(row));
                    }
                }
            }
        }
        _ => {
            let _ = writeln!(text, "{indent}{}", scalar(value));
        }
    }
}

/// Renders one scalar without quoting noise.
fn scalar(value: &Value) -> String {
    match value {
        Value::Null => "-".to_owned(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Applies the response bound to the complete rendered output.
///
/// The bound covers everything written, not only the projection: an envelope
/// carrying a large page or diagnostic list cannot push the result past the
/// bound while still reporting `truncated: false`.
///
/// Rows are dropped from the end of an array, and keys from the end of an
/// object, until the rendered output fits, so a bounded page degrades to fewer
/// rows rather than to silently missing content without the flag. The largest
/// fitting prefix is found by bisection rather than by removing one element at
/// a time, so a full page costs a handful of renders instead of one per row.
fn fit(data: &Value, render: impl Fn(&Value, bool) -> String) -> String {
    let full = render(data, false);
    if full.len() <= MAX_OUTPUT_BYTES {
        return full;
    }
    match data {
        Value::Array(rows) => {
            let keep = largest_fitting(rows.len(), |count| {
                render(&Value::Array(rows[..count].to_vec()), true).len() <= MAX_OUTPUT_BYTES
            });
            match keep {
                Some(count) => render(&Value::Array(rows[..count].to_vec()), true),
                None => omitted(&render),
            }
        }
        Value::Object(entries) => {
            let keys: Vec<String> = entries.keys().cloned().collect();
            let prefix = |count: usize| {
                let mut kept = Map::new();
                for key in keys.iter().take(count) {
                    if let Some(value) = entries.get(key) {
                        kept.insert(key.clone(), value.clone());
                    }
                }
                Value::Object(kept)
            };
            let keep = largest_fitting(keys.len(), |count| {
                render(&prefix(count), true).len() <= MAX_OUTPUT_BYTES
            });
            match keep {
                Some(count) => render(&prefix(count), true),
                None => omitted(&render),
            }
        }
        _ => omitted(&render),
    }
}

/// Renders the marker used when nothing of the projection fits.
fn omitted(render: &impl Fn(&Value, bool) -> String) -> String {
    render(
        &json!({ "omitted": "the value exceeds the response bound" }),
        true,
    )
}

/// Returns the largest `count` in `0..=len` for which `fits` holds.
///
/// `fits` is monotonic: a shorter prefix of the same value never renders
/// larger than a longer one. Returns `None` when even the empty prefix does not
/// fit, which means the envelope alone exceeds the bound.
fn largest_fitting(len: usize, fits: impl Fn(usize) -> bool) -> Option<usize> {
    if !fits(0) {
        return None;
    }
    let (mut low, mut high) = (0_usize, len);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if fits(middle) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Some(low)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use serde_json::json;

    use super::{
        Diagnostic, MAX_OUTPUT_BYTES, OUTPUT_SCHEMA_VERSION, PageInfo, Response, Writer,
        render_json,
    };
    use crate::args::OutputForm;
    use crate::command::Command;
    use crate::exit::ExitCategory;
    use crate::host::testing::TestHost;

    fn json_writer() -> Writer {
        Writer::new(OutputForm::Json, false)
    }

    #[test]
    fn the_envelope_carries_every_published_field() {
        let mut host = TestHost::new();
        let response = Response::success(Command::JobList, json!(["orders"]))
            .with_page(PageInfo {
                page_size: 50,
                returned: 1,
                next_cursor: None,
            })
            .with_diagnostic(Diagnostic::new("NOTE", "one page returned"));
        json_writer()
            .emit(&mut host, &response)
            .expect("the write succeeds");
        let value: serde_json::Value =
            serde_json::from_str(&host.stdout_text()).expect("the output is JSON");
        assert_eq!(value["schema_version"], json!(OUTPUT_SCHEMA_VERSION));
        assert_eq!(value["command"], json!("job list"));
        assert_eq!(value["outcome"], json!("success"));
        assert_eq!(value["data"], json!(["orders"]));
        assert_eq!(value["page"]["page_size"], json!(50));
        assert_eq!(value["page"]["returned"], json!(1));
        assert_eq!(value["page"]["next_cursor"], json!(null));
        assert_eq!(value["diagnostics"][0]["code"], json!("NOTE"));
        assert_eq!(value["truncated"], json!(false));
    }

    #[test]
    fn one_object_is_emitted_per_invocation() {
        let mut host = TestHost::new();
        let response = Response::success(Command::JobList, json!([]));
        json_writer()
            .emit(&mut host, &response)
            .expect("the write succeeds");
        assert_eq!(host.stdout_text().trim_end().lines().count(), 1);
    }

    #[test]
    fn a_failed_category_maps_to_its_outcome() {
        let mut host = TestHost::new();
        let response = Response::failed(
            Command::ExecutionStop,
            ExitCategory::OptimisticConflict,
            json!({ "rejection": "OPTIMISTIC_CONFLICT" }),
        );
        json_writer()
            .emit(&mut host, &response)
            .expect("the write succeeds");
        let value: serde_json::Value =
            serde_json::from_str(&host.stdout_text()).expect("the output is JSON");
        assert_eq!(value["outcome"], json!("conflict"));
    }

    #[test]
    fn exceeding_the_bound_sets_the_truncation_flag() {
        let mut host = TestHost::new();
        let row = "x".repeat(1024);
        let rows: Vec<serde_json::Value> = (0..512).map(|_| json!(row)).collect();
        let response = Response::success(Command::JobList, json!(rows));
        json_writer()
            .emit(&mut host, &response)
            .expect("the write succeeds");
        let value: serde_json::Value =
            serde_json::from_str(&host.stdout_text()).expect("the output is JSON");
        assert_eq!(value["truncated"], json!(true));
        let kept = value["data"].as_array().expect("data is an array").len();
        assert!(kept < 512, "the bound removed no row");
        assert!(value["data"].to_string().len() <= MAX_OUTPUT_BYTES);
    }

    #[test]
    fn the_bound_covers_the_whole_envelope_not_only_the_projection() {
        // The projection alone fits the bound; only the pagination and
        // diagnostic fields push the rendered envelope past it. Bounding just
        // the projection would therefore report `truncated: false` while
        // writing more than the bound allows.
        let mut host = TestHost::new();
        let row = "y".repeat(1024);
        let rows: Vec<serde_json::Value> = (0..250).map(|_| json!(row)).collect();
        let data = json!(rows);
        assert!(
            data.to_string().len() <= MAX_OUTPUT_BYTES,
            "the fixture is not discriminating: the projection alone already exceeds the bound"
        );

        let response = Response::success(Command::JobList, data.clone())
            .with_page(PageInfo {
                page_size: 500,
                returned: 250,
                next_cursor: Some("c".repeat(512)),
            })
            .with_diagnostic(Diagnostic::new("NOTE", "z".repeat(12 * 1024)));
        assert!(
            render_json(&response, &data, false).len() > MAX_OUTPUT_BYTES,
            "the fixture is not discriminating: the envelope already fits"
        );

        json_writer()
            .emit(&mut host, &response)
            .expect("the write succeeds");

        let written = host.stdout_text();
        assert!(
            written.len() <= MAX_OUTPUT_BYTES,
            "the envelope exceeded the bound at {} bytes",
            written.len()
        );
        let value: serde_json::Value = serde_json::from_str(&written).expect("the output is JSON");
        assert_eq!(value["truncated"], json!(true));
        // The fields that are not the projection survive truncation, because
        // the page and diagnostics are what a caller needs to continue.
        assert_eq!(value["page"]["page_size"], json!(500));
        assert_eq!(value["diagnostics"][0]["code"], json!("NOTE"));
        assert!(
            !value["data"]
                .as_array()
                .expect("data is an array")
                .is_empty(),
            "truncation removed the whole projection"
        );
    }

    #[test]
    fn a_result_within_the_bound_is_never_flagged_truncated() {
        let mut host = TestHost::new();
        let response = Response::success(Command::JobList, json!(["orders"])).with_page(PageInfo {
            page_size: 50,
            returned: 1,
            next_cursor: None,
        });
        json_writer()
            .emit(&mut host, &response)
            .expect("the write succeeds");
        let value: serde_json::Value =
            serde_json::from_str(&host.stdout_text()).expect("the output is JSON");
        assert_eq!(value["truncated"], json!(false));
        assert_eq!(value["data"], json!(["orders"]));
    }

    #[test]
    fn the_human_form_obeys_the_same_bound() {
        let mut host = TestHost::new();
        let row = "y".repeat(1024);
        let rows: Vec<serde_json::Value> = (0..512).map(|_| json!(row)).collect();
        let response = Response::success(Command::JobList, json!(rows));
        Writer::new(OutputForm::Human, false)
            .emit(&mut host, &response)
            .expect("the write succeeds");
        let written = host.stdout_text();
        assert!(written.len() <= MAX_OUTPUT_BYTES);
        assert!(written.contains("truncated: true"));
    }

    #[test]
    fn a_closed_pipe_reports_an_output_failure() {
        let mut host = TestHost::new().with_stdout_capacity(4);
        let response = Response::success(Command::JobList, json!(["orders", "invoices"]));
        json_writer()
            .emit(&mut host, &response)
            .expect_err("the pipe is closed");
        assert!(host.stdout_text().is_empty());
    }

    #[test]
    fn the_human_form_renders_without_styling_when_disabled() {
        let mut host = TestHost::new();
        let response = Response::success(
            Command::ExecutionShow,
            json!({ "execution_id": 4, "status": "COMPLETED" }),
        );
        Writer::new(OutputForm::Human, false)
            .emit(&mut host, &response)
            .expect("the write succeeds");
        let text = host.stdout_text();
        assert!(text.contains("execution_id: 4"));
        assert!(text.contains("status: COMPLETED"));
        assert!(!text.contains('\u{1b}'), "styling leaked into plain output");
    }
}
