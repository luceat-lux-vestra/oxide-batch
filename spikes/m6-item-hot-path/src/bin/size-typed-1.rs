//! Links one monomorphized pipeline.
//!
//! Subtracting this from `size-typed` gives the marginal code size and
//! compile time each additional native pipeline costs, which is the open
//! monomorphization-budget question in RFC-0005. `size-boxed-1` is the
//! matching baseline for the erased path.

use oxide_batch_m6_spikes::sizes::run_typed_pipeline;

fn main() {
    let fold = run_typed_pipeline::<0>(1_000, 100);
    println!("typed fold {fold}");
}
