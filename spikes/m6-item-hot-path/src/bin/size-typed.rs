//! Links sixteen distinct monomorphized pipelines.
//!
//! Paired with `size-boxed`, this binary is the code-size and compile-time
//! half of the RFC-0005 evidence. Both binaries run the same sixteen
//! pipelines over the same components; only dispatch differs.

use oxide_batch_m6_spikes::sizes::run_typed_pipeline;

macro_rules! pipelines {
    ($run:ident, $items:expr, $chunk:expr, $($index:literal),* $(,)?) => {{
        let mut fold = 0_u64;
        $( fold ^= $run::<{ $index }>($items, $chunk); )*
        fold
    }};
}

fn main() {
    let fold = pipelines!(
        run_typed_pipeline,
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
    println!("typed fold {fold}");
}
