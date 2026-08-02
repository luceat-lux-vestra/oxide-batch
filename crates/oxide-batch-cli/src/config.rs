//! Typed configuration with per-value precedence.
//!
//! Precedence is resolved per value rather than per source, so a file may
//! supply the repository pool size while an option supplies the page size.
//! Validation is strict and fail closed: unknown keys are errors, bounded
//! values are rejected outside their documented bounds, and every safe conflict
//! is reported in one pass before a repository connection is opened.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::args::{Arguments, OutputForm};
use crate::host::Host;

/// Configuration schema version accepted in a configuration file.
pub const CONFIG_VERSION: u64 = 1;

/// Largest configuration file the CLI reads.
const MAX_CONFIG_BYTES: usize = 256 * 1024;
/// Largest secret an indirection file may carry.
const MAX_SECRET_BYTES: usize = 64 * 1024;
/// Deepest accepted nesting in a configuration file.
const MAX_CONFIG_DEPTH: usize = 4;

const MIN_CLIENT_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_CLIENT_TIMEOUT: Duration = Duration::from_hours(1);
const DEFAULT_CLIENT_TIMEOUT: Duration = Duration::from_mins(1);
const DEFAULT_PAGE_SIZE: u16 = 50;
const MAX_PAGE_SIZE: u16 = 500;
const DEFAULT_POOL_SIZE: u32 = 10;
const MAX_POOL_SIZE: u32 = 1024;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_mins(5);
const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_STATEMENT_TIMEOUT: Duration = Duration::from_hours(24);
const MIN_BOUNDED_DURATION: Duration = Duration::from_millis(1);

/// Where one effective value came from.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Source {
    /// An explicit command-line option.
    Option,
    /// A namespaced environment variable.
    Environment,
    /// The configuration file.
    File,
    /// The documented framework default.
    Default,
}

impl Source {
    /// Returns the stable machine name of this source.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Option => "option",
            Self::Environment => "environment",
            Self::File => "file",
            Self::Default => "default",
        }
    }
}

impl fmt::Display for Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One effective value together with the source that supplied it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Resolved<T> {
    value: T,
    source: Source,
}

impl<T> Resolved<T> {
    const fn new(value: T, source: Source) -> Self {
        Self { value, source }
    }

    /// Borrows the effective value.
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Returns the source that supplied the value.
    #[must_use]
    pub const fn source(&self) -> Source {
        self.source
    }
}

impl<T: Copy> Resolved<T> {
    /// Returns the effective value.
    pub const fn get(&self) -> T {
        self.value
    }
}

/// A configuration value whose text must never reach output.
///
/// `Debug` and `Display` redact the value. `config show` prints the source and
/// a redaction marker instead.
#[derive(Clone, Eq, PartialEq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a secret value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the secret at an authorized boundary.
    ///
    /// The only authorized boundary in this crate is repository connection
    /// construction.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// Transport security selected for the repository connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsSetting {
    /// Validate the server certificate and hostname.
    #[default]
    VerifyFull,
    /// Use an unencrypted connection in an explicitly isolated environment.
    Plaintext,
}

impl TlsSetting {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "verify_full" => Some(Self::VerifyFull),
            "plaintext" => Some(Self::Plaintext),
            _ => None,
        }
    }

    /// Returns the stable machine name of this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VerifyFull => "verify_full",
            Self::Plaintext => "plaintext",
        }
    }
}

/// The closed set of configuration keys.
///
/// A key that is not in this table is rejected wherever it appears.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KeySpec {
    /// Dotted configuration path.
    path: &'static str,
    /// Namespaced environment variable.
    env: &'static str,
    /// Whether the value is secret bearing.
    secret: bool,
}

const KEYS: &[KeySpec] = &[
    KeySpec {
        path: "repository.url",
        env: "OXIDE_BATCH_REPOSITORY_URL",
        secret: true,
    },
    KeySpec {
        path: "repository.ca_certificate",
        env: "OXIDE_BATCH_REPOSITORY_CA_CERTIFICATE",
        secret: true,
    },
    KeySpec {
        path: "repository.tls_mode",
        env: "OXIDE_BATCH_REPOSITORY_TLS_MODE",
        secret: false,
    },
    KeySpec {
        path: "repository.pool_size",
        env: "OXIDE_BATCH_REPOSITORY_POOL_SIZE",
        secret: false,
    },
    KeySpec {
        path: "repository.connect_timeout",
        env: "OXIDE_BATCH_REPOSITORY_CONNECT_TIMEOUT",
        secret: false,
    },
    KeySpec {
        path: "repository.statement_timeout",
        env: "OXIDE_BATCH_REPOSITORY_STATEMENT_TIMEOUT",
        secret: false,
    },
    KeySpec {
        path: "output.form",
        env: "OXIDE_BATCH_OUTPUT_FORM",
        secret: false,
    },
    KeySpec {
        path: "output.page_size",
        env: "OXIDE_BATCH_OUTPUT_PAGE_SIZE",
        secret: false,
    },
    KeySpec {
        path: "client.timeout",
        env: "OXIDE_BATCH_CLIENT_TIMEOUT",
        secret: false,
    },
];

/// The suffix that supplies a value by file indirection instead of inline.
const FILE_SUFFIX: &str = "__FILE";

/// The effective configuration of one invocation.
#[derive(Clone, Debug)]
pub struct Configuration {
    repository_url: Option<Resolved<Secret>>,
    ca_certificate: Option<Resolved<Secret>>,
    tls_mode: Resolved<TlsSetting>,
    pool_size: Resolved<u32>,
    connect_timeout: Resolved<Duration>,
    statement_timeout: Resolved<Duration>,
    output: Resolved<OutputForm>,
    page_size: Resolved<u16>,
    client_timeout: Resolved<Duration>,
}

impl Configuration {
    /// Borrows the repository connection secret, when one was supplied.
    #[must_use]
    pub const fn repository_url(&self) -> Option<&Resolved<Secret>> {
        self.repository_url.as_ref()
    }

    /// Borrows the PEM certificate-authority bundle, when one was supplied.
    #[must_use]
    pub const fn ca_certificate(&self) -> Option<&Resolved<Secret>> {
        self.ca_certificate.as_ref()
    }

    /// Returns the selected transport security.
    #[must_use]
    pub const fn tls_mode(&self) -> TlsSetting {
        self.tls_mode.get()
    }

    /// Returns the validated connection pool bound.
    #[must_use]
    pub const fn pool_size(&self) -> u32 {
        self.pool_size.get()
    }

    /// Returns the validated connection establishment timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout.get()
    }

    /// Returns the validated server-side statement timeout.
    #[must_use]
    pub const fn statement_timeout(&self) -> Duration {
        self.statement_timeout.get()
    }

    /// Returns the effective output form.
    #[must_use]
    pub const fn output(&self) -> OutputForm {
        self.output.get()
    }

    /// Returns the effective page bound.
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.page_size.get()
    }

    /// Returns the effective client deadline.
    #[must_use]
    pub const fn client_timeout(&self) -> Duration {
        self.client_timeout.get()
    }

    /// Returns every effective value with its source and redaction status.
    ///
    /// The value column of a secret-bearing key is always the redaction
    /// marker, never the value.
    #[must_use]
    pub fn effective(&self) -> Vec<EffectiveValue> {
        let mut values = Vec::with_capacity(KEYS.len());
        if let Some(resolved) = &self.repository_url {
            values.push(EffectiveValue::secret("repository.url", resolved.source()));
        }
        if let Some(resolved) = &self.ca_certificate {
            values.push(EffectiveValue::secret(
                "repository.ca_certificate",
                resolved.source(),
            ));
        }
        values.push(EffectiveValue::plain(
            "repository.tls_mode",
            self.tls_mode.get().as_str().to_owned(),
            self.tls_mode.source(),
        ));
        values.push(EffectiveValue::plain(
            "repository.pool_size",
            self.pool_size.get().to_string(),
            self.pool_size.source(),
        ));
        values.push(EffectiveValue::plain(
            "repository.connect_timeout",
            format_duration(self.connect_timeout.get()),
            self.connect_timeout.source(),
        ));
        values.push(EffectiveValue::plain(
            "repository.statement_timeout",
            format_duration(self.statement_timeout.get()),
            self.statement_timeout.source(),
        ));
        values.push(EffectiveValue::plain(
            "output.form",
            self.output.get().as_str().to_owned(),
            self.output.source(),
        ));
        values.push(EffectiveValue::plain(
            "output.page_size",
            self.page_size.get().to_string(),
            self.page_size.source(),
        ));
        values.push(EffectiveValue::plain(
            "client.timeout",
            format_duration(self.client_timeout.get()),
            self.client_timeout.source(),
        ));
        values.sort_by(|left, right| left.key.cmp(&right.key));
        values
    }
}

/// One row of `config show`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectiveValue {
    key: String,
    value: String,
    source: Source,
    redacted: bool,
}

impl EffectiveValue {
    fn plain(key: &str, value: String, source: Source) -> Self {
        Self {
            key: key.to_owned(),
            value,
            source,
            redacted: false,
        }
    }

    fn secret(key: &str, source: Source) -> Self {
        Self {
            key: key.to_owned(),
            value: "<redacted>".to_owned(),
            source,
            redacted: true,
        }
    }

    /// Borrows the dotted configuration key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Borrows the displayable value or its redaction marker.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the source that supplied the value.
    #[must_use]
    pub const fn source(&self) -> Source {
        self.source
    }

    /// Returns whether the displayed value is a redaction marker.
    #[must_use]
    pub const fn is_redacted(&self) -> bool {
        self.redacted
    }
}

/// One rejected configuration value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigIssue {
    key: String,
    detail: String,
}

impl ConfigIssue {
    fn new(key: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            detail: detail.into(),
        }
    }

    /// Borrows the rejected key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Borrows the safe-to-display reason.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.key, self.detail)
    }
}

/// Every safe-to-display configuration conflict found in one pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    issues: Vec<ConfigIssue>,
}

impl ConfigError {
    fn single(issue: ConfigIssue) -> Self {
        Self {
            issues: vec![issue],
        }
    }

    /// Borrows every reported issue.
    #[must_use]
    pub fn issues(&self) -> &[ConfigIssue] {
        &self.issues
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for issue in &self.issues {
            if !first {
                formatter.write_str("; ")?;
            }
            issue.fmt(formatter)?;
            first = false;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

/// Resolves the effective configuration of one invocation.
///
/// Resolution never opens a repository connection, so a configuration error is
/// always reported before any connection attempt.
///
/// # Errors
///
/// Returns every safe-to-display unknown key, malformed value, out-of-bounds
/// value, or unreadable indirection file found in one pass.
pub fn resolve<H: Host>(host: &H, arguments: &Arguments) -> Result<Configuration, ConfigError> {
    let mut issues = Vec::new();
    let file = match load_file(host, arguments.config.as_deref()) {
        Ok(values) => values,
        Err(error) => {
            // A file that cannot be parsed makes every file-sourced value
            // unknowable, so resolution stops rather than silently falling
            // back to defaults.
            return Err(error);
        }
    };

    let repository = resolve_repository(host, &file, &mut issues);

    let output = enum_value_with_option(
        host,
        &file,
        &mut issues,
        "output.form",
        arguments.output.as_deref(),
        parse_output_form,
        "human or json",
    )
    .unwrap_or_else(|| Resolved::new(OutputForm::default(), Source::Default));

    let page_size = bounded_u16(
        host,
        &file,
        &mut issues,
        "output.page_size",
        arguments.page_size.as_deref(),
        1,
        MAX_PAGE_SIZE,
    )
    .unwrap_or_else(|| Resolved::new(DEFAULT_PAGE_SIZE, Source::Default));

    let client_timeout = bounded_duration(
        host,
        &file,
        &mut issues,
        "client.timeout",
        arguments.timeout.as_deref(),
        MIN_CLIENT_TIMEOUT,
        MAX_CLIENT_TIMEOUT,
    )
    .unwrap_or_else(|| Resolved::new(DEFAULT_CLIENT_TIMEOUT, Source::Default));

    if issues.is_empty() {
        Ok(Configuration {
            repository_url: repository.url,
            ca_certificate: repository.ca_certificate,
            tls_mode: repository.tls_mode,
            pool_size: repository.pool_size,
            connect_timeout: repository.connect_timeout,
            statement_timeout: repository.statement_timeout,
            output,
            page_size,
            client_timeout,
        })
    } else {
        Err(ConfigError { issues })
    }
}

/// The repository-class values of one invocation.
///
/// These are deployment controlled and secret bearing, and no command-line
/// option supplies any of them, so they resolve from the environment, the
/// configuration file, or a documented default only.
struct RepositorySettings {
    url: Option<Resolved<Secret>>,
    ca_certificate: Option<Resolved<Secret>>,
    tls_mode: Resolved<TlsSetting>,
    pool_size: Resolved<u32>,
    connect_timeout: Resolved<Duration>,
    statement_timeout: Resolved<Duration>,
}

fn resolve_repository<H: Host>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
) -> RepositorySettings {
    let url = string_value(host, file, issues, "repository.url", None)
        .map(|resolved| Resolved::new(Secret::new(resolved.value), resolved.source));
    let ca_certificate = string_value(host, file, issues, "repository.ca_certificate", None)
        .map(|resolved| Resolved::new(Secret::new(resolved.value), resolved.source));
    let tls_mode = enum_value(
        host,
        file,
        issues,
        "repository.tls_mode",
        TlsSetting::parse,
        "verify_full or plaintext",
    )
    .unwrap_or_else(|| Resolved::new(TlsSetting::default(), Source::Default));
    let pool_size = bounded_u32(
        host,
        file,
        issues,
        "repository.pool_size",
        None,
        1,
        MAX_POOL_SIZE,
    )
    .unwrap_or_else(|| Resolved::new(DEFAULT_POOL_SIZE, Source::Default));
    let connect_timeout = bounded_duration(
        host,
        file,
        issues,
        "repository.connect_timeout",
        None,
        MIN_BOUNDED_DURATION,
        MAX_CONNECT_TIMEOUT,
    )
    .unwrap_or_else(|| Resolved::new(DEFAULT_CONNECT_TIMEOUT, Source::Default));
    let statement_timeout = bounded_duration(
        host,
        file,
        issues,
        "repository.statement_timeout",
        None,
        MIN_BOUNDED_DURATION,
        MAX_STATEMENT_TIMEOUT,
    )
    .unwrap_or_else(|| Resolved::new(DEFAULT_STATEMENT_TIMEOUT, Source::Default));
    RepositorySettings {
        url,
        ca_certificate,
        tls_mode,
        pool_size,
        connect_timeout,
        statement_timeout,
    }
}

fn parse_output_form(value: &str) -> Option<OutputForm> {
    match value {
        "human" => Some(OutputForm::Human),
        "json" => Some(OutputForm::Json),
        _ => None,
    }
}

/// A raw value and the source that supplied it.
struct RawValue {
    value: String,
    source: Source,
}

/// Applies per-value precedence for one key.
///
/// The first source that supplies the key wins, and a lower-priority source is
/// not consulted for that key even though it may still supply another.
fn raw_value<H: Host>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    option: Option<&str>,
) -> Option<RawValue> {
    if let Some(value) = option {
        return Some(RawValue {
            value: value.to_owned(),
            source: Source::Option,
        });
    }
    let spec = KEYS.iter().find(|key| key.path == path)?;
    if let Some(value) = host.env(spec.env) {
        return Some(RawValue {
            value,
            source: Source::Environment,
        });
    }
    let env_file = format!("{}{FILE_SUFFIX}", spec.env);
    if let Some(path_value) = host.env(&env_file) {
        return read_secret_file(
            host,
            issues,
            path,
            Path::new(&path_value),
            Source::Environment,
        );
    }
    if let Some(value) = file.get(path) {
        return Some(RawValue {
            value: value.clone(),
            source: Source::File,
        });
    }
    let file_key = format!("{path}{FILE_SUFFIX}");
    if let Some(path_value) = file.get(&file_key) {
        return read_secret_file(host, issues, path, Path::new(path_value), Source::File);
    }
    None
}

/// Reads a value supplied by file indirection.
fn read_secret_file<H: Host>(
    host: &H,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    file: &Path,
    source: Source,
) -> Option<RawValue> {
    match host.read_file(file) {
        Ok(bytes) if bytes.len() > MAX_SECRET_BYTES => {
            issues.push(ConfigIssue::new(
                path,
                format!("the indirection file exceeds {MAX_SECRET_BYTES} bytes"),
            ));
            None
        }
        Ok(bytes) => {
            if let Ok(value) = String::from_utf8(bytes) {
                Some(RawValue {
                    value: value.trim_end_matches(['\n', '\r']).to_owned(),
                    source,
                })
            } else {
                issues.push(ConfigIssue::new(
                    path,
                    "the indirection file is not valid UTF-8",
                ));
                None
            }
        }
        Err(_) => {
            // The path itself is never echoed, because a certificate or
            // credential path is excluded from diagnostics.
            issues.push(ConfigIssue::new(path, "the indirection file is unreadable"));
            None
        }
    }
}

fn string_value<H: Host>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    option: Option<&str>,
) -> Option<RawValue> {
    let raw = raw_value(host, file, issues, path, option)?;
    if raw.value.is_empty() {
        issues.push(ConfigIssue::new(path, "the value must not be empty"));
        return None;
    }
    Some(raw)
}

fn enum_value<H: Host, T>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    parse: fn(&str) -> Option<T>,
    expected: &str,
) -> Option<Resolved<T>> {
    enum_value_with_option(host, file, issues, path, None, parse, expected)
}

fn enum_value_with_option<H: Host, T>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    option: Option<&str>,
    parse: fn(&str) -> Option<T>,
    expected: &str,
) -> Option<Resolved<T>> {
    let raw = raw_value(host, file, issues, path, option)?;
    if let Some(value) = parse(&raw.value) {
        Some(Resolved::new(value, raw.source))
    } else {
        issues.push(ConfigIssue::new(path, format!("expected {expected}")));
        None
    }
}

fn bounded_u16<H: Host>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    option: Option<&str>,
    min: u16,
    max: u16,
) -> Option<Resolved<u16>> {
    let raw = raw_value(host, file, issues, path, option)?;
    match raw.value.parse::<u16>() {
        Ok(value) if (min..=max).contains(&value) => Some(Resolved::new(value, raw.source)),
        _ => {
            issues.push(ConfigIssue::new(
                path,
                format!("expected an integer in {min}..={max}"),
            ));
            None
        }
    }
}

fn bounded_u32<H: Host>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    option: Option<&str>,
    min: u32,
    max: u32,
) -> Option<Resolved<u32>> {
    let raw = raw_value(host, file, issues, path, option)?;
    match raw.value.parse::<u32>() {
        Ok(value) if (min..=max).contains(&value) => Some(Resolved::new(value, raw.source)),
        _ => {
            issues.push(ConfigIssue::new(
                path,
                format!("expected an integer in {min}..={max}"),
            ));
            None
        }
    }
}

fn bounded_duration<H: Host>(
    host: &H,
    file: &BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
    path: &str,
    option: Option<&str>,
    min: Duration,
    max: Duration,
) -> Option<Resolved<Duration>> {
    let raw = raw_value(host, file, issues, path, option)?;
    match parse_duration(&raw.value) {
        Some(value) if value >= min && value <= max => Some(Resolved::new(value, raw.source)),
        _ => {
            issues.push(ConfigIssue::new(
                path,
                format!(
                    "expected a duration in {}..={}",
                    format_duration(min),
                    format_duration(max)
                ),
            ));
            None
        }
    }
}

/// Parses a bounded duration written as an integer and a unit.
///
/// The accepted units are `ms`, `s`, `m`, `h`, and `d`. A bare integer is
/// rejected so that a unit is always explicit.
fn parse_duration(value: &str) -> Option<Duration> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .filter(|index| *index > 0)?;
    let (digits, unit) = value.split_at(split);
    let amount: u64 = digits.parse().ok()?;
    let millis = match unit {
        "ms" => amount,
        "s" => amount.checked_mul(1_000)?,
        "m" => amount.checked_mul(60 * 1_000)?,
        "h" => amount.checked_mul(60 * 60 * 1_000)?,
        "d" => amount.checked_mul(24 * 60 * 60 * 1_000)?,
        _ => return None,
    };
    Some(Duration::from_millis(millis))
}

/// Renders a duration in the largest unit that divides it exactly.
fn format_duration(value: Duration) -> String {
    let millis = u64::try_from(value.as_millis()).unwrap_or(u64::MAX);
    for (unit, scale) in [
        ("d", 24 * 60 * 60 * 1_000_u64),
        ("h", 60 * 60 * 1_000),
        ("m", 60 * 1_000),
        ("s", 1_000),
    ] {
        if millis >= scale && millis % scale == 0 {
            return format!("{}{unit}", millis / scale);
        }
    }
    format!("{millis}ms")
}

/// Reads and flattens the configuration file, if one was named.
fn load_file<H: Host>(
    host: &H,
    path: Option<&Path>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let Some(path) = path else {
        return Ok(BTreeMap::new());
    };
    let mode = host.file_mode(path).map_err(|_| {
        ConfigError::single(ConfigIssue::new(
            "config",
            "the configuration file is unreadable",
        ))
    })?;
    if let Some(mode) = mode
        && mode & 0o077 != 0
    {
        return Err(ConfigError::single(ConfigIssue::new(
            "config",
            "the configuration file is group or world readable",
        )));
    }
    let bytes = host.read_file(path).map_err(|_| {
        ConfigError::single(ConfigIssue::new(
            "config",
            "the configuration file is unreadable",
        ))
    })?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(ConfigError::single(ConfigIssue::new(
            "config",
            format!("the configuration file exceeds {MAX_CONFIG_BYTES} bytes"),
        )));
    }
    let document: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
        ConfigError::single(ConfigIssue::new(
            "config",
            "the configuration file is not valid JSON",
        ))
    })?;
    flatten(&document)
}

/// Flattens the document and rejects every key outside the closed set.
fn flatten(document: &serde_json::Value) -> Result<BTreeMap<String, String>, ConfigError> {
    let serde_json::Value::Object(root) = document else {
        return Err(ConfigError::single(ConfigIssue::new(
            "config",
            "the configuration file must be a JSON object",
        )));
    };
    let mut issues = Vec::new();
    match root
        .get("config_version")
        .and_then(serde_json::Value::as_u64)
    {
        Some(version) if version == CONFIG_VERSION => {}
        Some(_) => issues.push(ConfigIssue::new(
            "config_version",
            format!("expected version {CONFIG_VERSION}"),
        )),
        None => issues.push(ConfigIssue::new(
            "config_version",
            format!("the configuration file must declare version {CONFIG_VERSION}"),
        )),
    }
    let mut values = BTreeMap::new();
    for (name, value) in root {
        if name == "config_version" {
            continue;
        }
        collect(name, value, 1, &mut values, &mut issues);
    }
    for key in values.keys() {
        let base = key.strip_suffix(FILE_SUFFIX).unwrap_or(key);
        let Some(spec) = KEYS.iter().find(|candidate| candidate.path == base) else {
            issues.push(ConfigIssue::new(key.clone(), "unknown configuration key"));
            continue;
        };
        if key.ends_with(FILE_SUFFIX) && !spec.secret {
            issues.push(ConfigIssue::new(
                key.clone(),
                "file indirection applies only to a secret-bearing key",
            ));
        }
        if values.contains_key(base) && values.contains_key(&format!("{base}{FILE_SUFFIX}")) {
            issues.push(ConfigIssue::new(
                base.to_owned(),
                "the inline value and its file indirection cannot both be supplied",
            ));
        }
    }
    if issues.is_empty() {
        Ok(values)
    } else {
        issues.sort_by(|left, right| left.key.cmp(&right.key));
        issues.dedup();
        Err(ConfigError { issues })
    }
}

/// Walks one configuration subtree into dotted keys.
fn collect(
    prefix: &str,
    value: &serde_json::Value,
    depth: usize,
    values: &mut BTreeMap<String, String>,
    issues: &mut Vec<ConfigIssue>,
) {
    if depth > MAX_CONFIG_DEPTH {
        issues.push(ConfigIssue::new(
            prefix.to_owned(),
            format!("the configuration file nests deeper than {MAX_CONFIG_DEPTH} levels"),
        ));
        return;
    }
    match value {
        serde_json::Value::Object(entries) => {
            for (name, entry) in entries {
                collect(
                    &format!("{prefix}.{name}"),
                    entry,
                    depth + 1,
                    values,
                    issues,
                );
            }
        }
        serde_json::Value::String(text) => {
            values.insert(prefix.to_owned(), text.clone());
        }
        serde_json::Value::Number(number) => {
            values.insert(prefix.to_owned(), number.to_string());
        }
        serde_json::Value::Bool(flag) => {
            values.insert(prefix.to_owned(), flag.to_string());
        }
        serde_json::Value::Null | serde_json::Value::Array(_) => {
            issues.push(ConfigIssue::new(
                prefix.to_owned(),
                "expected a string, number, or boolean",
            ));
        }
    }
}

/// Parses a bounded duration written as an integer and a unit.
///
/// The accepted units are `ms`, `s`, `m`, `h`, and `d`. This is the same
/// grammar configuration values use, so an age bound and a timeout are written
/// the same way.
#[must_use]
pub fn parse_public_duration(value: &str) -> Option<Duration> {
    parse_duration(value)
}

/// Returns the environment variable that supplies one configuration key.
#[must_use]
pub fn environment_variable(path: &str) -> Option<&'static str> {
    KEYS.iter().find(|key| key.path == path).map(|key| key.env)
}

/// Returns every accepted configuration key in canonical order.
#[must_use]
pub fn known_keys() -> Vec<&'static str> {
    let mut keys: Vec<&'static str> = KEYS.iter().map(|key| key.path).collect();
    keys.sort_unstable();
    keys
}

/// Returns the canonical configuration file path a deployment may use.
#[must_use]
pub fn default_config_path() -> PathBuf {
    PathBuf::from("oxide-batch.json")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::{Source, TlsSetting, format_duration, parse_duration, resolve};
    use crate::args::{Arguments, OutputForm};
    use crate::host::testing::TestHost;
    use std::path::PathBuf;
    use std::time::Duration;

    fn arguments() -> Arguments {
        Arguments::default()
    }

    #[test]
    fn defaults_apply_without_any_source() {
        let host = TestHost::new();
        let config = resolve(&host, &arguments()).expect("defaults are valid");
        assert_eq!(config.page_size(), 50);
        assert_eq!(config.output(), OutputForm::Human);
        assert_eq!(config.client_timeout(), Duration::from_mins(1));
        assert_eq!(config.tls_mode(), TlsSetting::VerifyFull);
    }

    #[test]
    fn an_option_outranks_the_environment() {
        let host = TestHost::new().with_env("OXIDE_BATCH_OUTPUT_PAGE_SIZE", "10");
        let mut arguments = arguments();
        arguments.page_size = Some("25".to_owned());
        let config = resolve(&host, &arguments).expect("the value is valid");
        assert_eq!(config.page_size(), 25);
        assert_eq!(config.effective_source("output.page_size"), Source::Option);
    }

    #[test]
    fn precedence_is_resolved_per_value() {
        let host = TestHost::new()
            .with_file(
                "/etc/oxide-batch.json",
                r#"{"config_version":1,"repository":{"pool_size":7},"output":{"page_size":10}}"#,
            )
            .with_env("OXIDE_BATCH_OUTPUT_FORM", "json");
        let mut arguments = arguments();
        arguments.config = Some(PathBuf::from("/etc/oxide-batch.json"));
        arguments.page_size = Some("25".to_owned());
        let config = resolve(&host, &arguments).expect("the values are valid");

        assert_eq!(config.page_size(), 25);
        assert_eq!(config.effective_source("output.page_size"), Source::Option);
        assert_eq!(config.output(), OutputForm::Json);
        assert_eq!(config.effective_source("output.form"), Source::Environment);
        assert_eq!(config.pool_size(), 7);
        assert_eq!(
            config.effective_source("repository.pool_size"),
            Source::File
        );
        assert_eq!(config.client_timeout(), Duration::from_mins(1));
        assert_eq!(config.effective_source("client.timeout"), Source::Default);
    }

    #[test]
    fn an_unknown_configuration_key_fails() {
        let host = TestHost::new().with_file(
            "/etc/oxide-batch.json",
            r#"{"config_version":1,"output":{"colour":"green"}}"#,
        );
        let mut arguments = arguments();
        arguments.config = Some(PathBuf::from("/etc/oxide-batch.json"));
        let error = resolve(&host, &arguments).expect_err("the key is unknown");
        assert!(
            error
                .issues()
                .iter()
                .any(|issue| issue.key() == "output.colour")
        );
    }

    #[test]
    fn a_world_readable_configuration_file_is_rejected() {
        let host = TestHost::new()
            .with_file("/etc/oxide-batch.json", r#"{"config_version":1}"#)
            .with_mode("/etc/oxide-batch.json", 0o644);
        let mut arguments = arguments();
        arguments.config = Some(PathBuf::from("/etc/oxide-batch.json"));
        let error = resolve(&host, &arguments).expect_err("the file is too permissive");
        assert!(
            error
                .issues()
                .iter()
                .any(|issue| issue.detail().contains("group or world readable"))
        );
    }

    #[test]
    fn a_missing_configuration_version_fails() {
        let host =
            TestHost::new().with_file("/etc/oxide-batch.json", r#"{"output":{"form":"json"}}"#);
        let mut arguments = arguments();
        arguments.config = Some(PathBuf::from("/etc/oxide-batch.json"));
        let error = resolve(&host, &arguments).expect_err("the version is required");
        assert!(
            error
                .issues()
                .iter()
                .any(|issue| issue.key() == "config_version")
        );
    }

    #[test]
    fn every_safe_conflict_is_reported_in_one_pass() {
        let host = TestHost::new()
            .with_env("OXIDE_BATCH_OUTPUT_PAGE_SIZE", "9000")
            .with_env("OXIDE_BATCH_CLIENT_TIMEOUT", "4h")
            .with_env("OXIDE_BATCH_REPOSITORY_TLS_MODE", "maybe");
        let error = resolve(&host, &arguments()).expect_err("the values are out of bounds");
        assert_eq!(error.issues().len(), 3);
    }

    #[test]
    fn a_secret_is_read_by_file_indirection() {
        let host = TestHost::new()
            .with_file("/run/secrets/url", "postgres://localhost/batch\n")
            .with_env("OXIDE_BATCH_REPOSITORY_URL__FILE", "/run/secrets/url");
        let config = resolve(&host, &arguments()).expect("the secret is readable");
        let url = config.repository_url().expect("the url is present");
        assert_eq!(url.value().expose(), "postgres://localhost/batch");
        assert_eq!(url.source(), Source::Environment);
    }

    #[test]
    fn a_secret_never_renders_its_value() {
        let host =
            TestHost::new().with_env("OXIDE_BATCH_REPOSITORY_URL", "postgres://secret@host/db");
        let config = resolve(&host, &arguments()).expect("the secret is valid");
        let url = config.repository_url().expect("the url is present");
        assert_eq!(format!("{}", url.value()), "<redacted>");
        assert_eq!(format!("{:?}", url.value()), "<redacted>");
        let rendered = config
            .effective()
            .into_iter()
            .find(|value| value.key() == "repository.url")
            .expect("the row is present");
        assert!(rendered.is_redacted());
        assert_eq!(rendered.value(), "<redacted>");
    }

    #[test]
    fn durations_round_trip_through_their_largest_exact_unit() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_mins(5)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_hours(1)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("30"), None);
        assert_eq!(parse_duration("s"), None);
        assert_eq!(parse_duration("30x"), None);
        assert_eq!(format_duration(Duration::from_mins(5)), "5m");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1500ms");
    }

    impl super::Configuration {
        fn effective_source(&self, key: &str) -> Source {
            self.effective()
                .into_iter()
                .find(|value| value.key() == key)
                .map_or(Source::Default, |value| value.source())
        }
    }
}
