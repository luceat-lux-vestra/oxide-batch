// The workspace forbids unsafe code and this crate denies it. Counting heap
// allocations per item is the central RFC-0005 measurement, and a global
// allocator is the only way to observe it without adding a dependency. The
// relaxation is confined to this module and this private crate.
#![allow(unsafe_code)]

//! A counting global allocator used to measure per-item heap traffic.
//!
//! Counting is off until [`begin`] is called, so process startup, workload
//! construction, and the test harness are excluded.
//!
//! A window belongs to the thread that opened it. Global counters would also
//! count the test harness's own bookkeeping, which runs on another thread
//! while the measured run is in flight: an otherwise identical run reported
//! `2` allocations of `144` bytes on a loaded CI host and `0` everywhere else.
//! A measurement that depends on what a different thread happened to do is not
//! reproducible, so the window, the counters, and the allocations they see all
//! belong to one thread.
//!
//! The thread-local state is const-initialized and holds no destructor, so
//! reading it inside the allocator cannot allocate and cannot reenter.
//!
//! Measurement windows are opened inside the asynchronous body rather than
//! around the executor. The spike components never yield, so a whole run
//! completes within one poll and no scheduler traffic is attributed to a path.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

/// A pass-through allocator that counts allocations while a window is open.
pub struct CountingAllocator;

impl CountingAllocator {
    fn record(size: usize) {
        if !ENABLED.try_with(Cell::get).unwrap_or(false) {
            return;
        }
        ALLOCATIONS.with(|allocations| allocations.set(allocations.get().saturating_add(1)));
        BYTES.with(|bytes| {
            bytes.set(
                bytes
                    .get()
                    .saturating_add(u64::try_from(size).unwrap_or(u64::MAX)),
            );
        });
    }
}

// SAFETY: every method forwards to the system allocator with the same pointer
// and layout it received, and the counters are plain atomics that never
// allocate or reenter.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// One closed measurement window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Measurement {
    /// Successful and attempted allocation calls observed in the window.
    pub allocations: u64,
    /// Requested bytes observed in the window.
    pub bytes: u64,
}

impl Measurement {
    /// Returns allocations divided by `items`, or `None` for an empty run.
    #[must_use]
    pub fn per_item(self, items: u64) -> Option<f64> {
        if items == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some(self.allocations as f64 / items as f64)
    }
}

/// Resets the counters and opens a measurement window on this thread.
pub fn begin() {
    ALLOCATIONS.with(|allocations| allocations.set(0));
    BYTES.with(|bytes| bytes.set(0));
    ENABLED.with(|enabled| enabled.set(true));
}

/// Closes this thread's window and returns what was counted.
#[must_use]
pub fn end() -> Measurement {
    ENABLED.with(|enabled| enabled.set(false));
    Measurement {
        allocations: ALLOCATIONS.with(Cell::get),
        bytes: BYTES.with(Cell::get),
    }
}
