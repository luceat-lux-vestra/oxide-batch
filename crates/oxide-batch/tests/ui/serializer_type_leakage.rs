//! Durable-state contracts do not expose or re-export serializer types.

use oxide_batch::serde_json::Value;

fn main() {
    let _: Option<Value> = None;
}
