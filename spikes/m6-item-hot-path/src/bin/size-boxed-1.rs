//! Links one pipeline behind `dyn` adapters.
//!
//! The erased baseline for the marginal-cost comparison described in
//! `size-typed-1`.

use oxide_batch_m6_spikes::sizes::run_boxed_pipeline;

fn main() {
    let fold = run_boxed_pipeline::<0>(1_000, 100);
    println!("boxed fold {fold}");
}
