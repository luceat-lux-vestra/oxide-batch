//! Links sixteen distinct pipelines behind `dyn` adapters.
//!
//! The erased counterpart of `size-typed`. The driver is still instantiated
//! once per item type, so the difference between the two binaries isolates
//! what monomorphizing the component calls costs.

use oxide_batch_m6_spikes::sizes::run_boxed_pipeline;

macro_rules! pipelines {
    ($run:ident, $items:expr, $chunk:expr, $($index:literal),* $(,)?) => {{
        let mut fold = 0_u64;
        $( fold ^= $run::<{ $index }>($items, $chunk); )*
        fold
    }};
}

fn main() {
    let fold = pipelines!(
        run_boxed_pipeline,
        1_000,
        100,
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
    );
    println!("boxed fold {fold}");
}
