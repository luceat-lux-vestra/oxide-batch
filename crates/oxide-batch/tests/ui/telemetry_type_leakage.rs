//! Telemetry contracts are facade-owned and re-export no telemetry SDK.

use oxide_batch::opentelemetry::metrics::Meter;
use oxide_batch::tracing_subscriber::Registry;

fn main() {
    let _: Option<Meter> = None;
    let _: Option<Registry> = None;
}
