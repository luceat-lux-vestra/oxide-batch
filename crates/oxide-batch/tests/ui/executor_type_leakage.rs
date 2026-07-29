//! The facade owns async contracts and does not re-export executor types.

use oxide_batch::tokio::runtime::Handle;

fn main() {
    let _: Option<Handle> = None;
}
