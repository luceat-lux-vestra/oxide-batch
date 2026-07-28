/// A non-production marker used to prove diagnostic sinks redact values.
pub const SENTINEL_SECRET: &str = "oxide-batch-sentinel-secret-4f2d91";

/// Asserts that every named diagnostic sink excludes the sentinel secret.
///
/// Sink names are shown on failure; sink contents are deliberately omitted.
pub fn assert_sentinel_absent<'a>(sinks: impl IntoIterator<Item = (&'a str, &'a str)>) {
    for (sink_name, contents) in sinks {
        assert!(
            !contents.contains(SENTINEL_SECRET),
            "sentinel secret leaked through diagnostic sink {sink_name}"
        );
    }
}
