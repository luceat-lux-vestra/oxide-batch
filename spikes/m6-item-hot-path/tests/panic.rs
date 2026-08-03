//! A panic in a component must unwind identically through both dispatch
//! forms.
//!
//! ADR-0002 and spike 0001 already fixed how the framework classifies a
//! component panic. What matters here is narrower: erasure must not swallow,
//! reorder, or rewrite a panic, and the monomorphized path must not turn one
//! into an abort. Each case runs on its own current-thread runtime inside
//! `catch_unwind`. The panic hook is process-global, so this binary must run
//! with `--test-threads=1`.

#![allow(clippy::expect_used)]

use std::panic::{self, AssertUnwindSafe};

use oxide_batch_m6_spikes::scenario::{Scenario, execute_boxed, execute_typed};
use oxide_batch_m6_spikes::workload::Fault;

fn payload(error: &(dyn std::any::Any + Send)) -> String {
    error.downcast_ref::<String>().map_or_else(
        || {
            error.downcast_ref::<&str>().map_or_else(
                || "<opaque panic payload>".to_owned(),
                |text| (*text).to_owned(),
            )
        },
        Clone::clone,
    )
}

fn catch(scenario: Scenario, boxed: bool) -> Option<String> {
    let previous = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime must build");
        if boxed {
            runtime.block_on(execute_boxed(scenario));
        } else {
            runtime.block_on(execute_typed(scenario));
        }
    }));

    panic::set_hook(previous);
    result.err().map(|error| payload(error.as_ref()))
}

fn assert_identical_panic(scenario: Scenario, expected: &str) {
    let typed = catch(scenario, false);
    let boxed = catch(scenario, true);

    assert_eq!(
        typed.as_deref(),
        Some(expected),
        "the typed path did not panic as expected"
    );
    assert_eq!(
        typed, boxed,
        "the two dispatch forms disagreed about the panic"
    );
}

/// All cases live in one test because the panic hook is process-global and
/// concurrent tests would swap it underneath each other.
#[test]
fn component_panics_unwind_identically_on_both_paths() {
    assert_identical_panic(
        Scenario::new(64, 8).with_reader_fault(Fault::Panic(21)),
        "reader panic at item 21",
    );
    assert_identical_panic(
        Scenario::new(64, 8).with_processor_fault(Fault::Panic(21)),
        "processor panic at item 21",
    );
    assert_identical_panic(
        Scenario::new(64, 8).with_writer_fault(Fault::Panic(3)),
        "writer panic at batch 3",
    );

    let clean = Scenario::new(64, 8);
    assert_eq!(catch(clean, false), None);
    assert_eq!(catch(clean, true), None);
}
