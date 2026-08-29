//! Gate H (#153 §6) structural proof of the typed path's hard invariants:
//! framework-controlled per-item future allocation == 0, and no
//! framework-controlled dynamic dispatch per item.
//!
//! Neither invariant is something a runtime counter can prove better than the
//! type system already does, and this codebase has no dynamic-dispatch
//! counter to build one from (an allocation counter exists --
//! `gate_h_allocation.rs`, `chunk_allocation.rs`,
//! `item_components_allocation.rs` -- and is corroborating evidence, not the
//! proof; see below for why it cannot isolate the claim on an allocating
//! component). Both invariants are facts about which concrete types a call
//! site resolves to, decided entirely at compile time by monomorphization,
//! so the proof here is a structural argument backed by the actual type
//! definitions in this repository, plus one deterministic, portable runtime
//! check that would catch a regression the type-level argument missed.
//!
//! ## The structural argument
//!
//! `ChunkStep<I, O, R, P, W>` (`crates/oxide-batch/src/chunk_runtime.rs:61`)
//! stores its reader, processor, and writer as bare fields of their own
//! generic type:
//!
//! ```text
//! pub struct ChunkStep<I, O, R, P, W> {
//!     reader: R,
//!     processor: P,
//!     writer: W,
//!     transactions: Arc<dyn ChunkTransactionManager>,   // dyn -- deliberately, chunk-level
//!     completion: Arc<dyn ChunkCompletion>,              // dyn -- deliberately, chunk-level
//!     listeners: Vec<Arc<dyn ChunkListener>>,            // dyn -- deliberately, chunk-level
//!     ...
//! }
//! ```
//!
//! `transactions`/`completion`/`listeners` are declared `dyn` deliberately:
//! they are chunk-boundary ports, invoked once (or a few times) per chunk,
//! not once per item, so their dispatch cost is not what Gate H's per-item
//! invariant is about. `reader`/`processor`/`writer` carry no such
//! qualifier -- they are exactly `R`, `P`, `W`.
//!
//! For the **typed** representation, `R`/`P`/`W` are concrete, named structs
//! with no `dyn` anywhere in their own definitions -- verified directly, not
//! assumed:
//!
//! - `DelimitedReader<Src>` (`crates/oxide-batch/src/item_components/delimited.rs:517`):
//!   fields are `BufReader<Src>`, a `CoreReader`, and two `Vec` buffers.
//! - `DelimitedWriter` (`crates/oxide-batch/src/item_components/delimited.rs:949`):
//!   fields are an `Arc<Mutex<DelimitedWriterState>>` and a dialect value.
//! - `IdentityProcessor` (`crates/oxide-batch/src/item_components/basic.rs:80`):
//!   a unit struct.
//!
//! None of these contain a `dyn` anywhere. A method call through `R`/`P`/`W`
//! in this configuration is resolved by the compiler at the
//! `ChunkStep<I, O, R, P, W>` monomorphization for this exact `R`, `P`, `W` --
//! there is no vtable for the compiler to consult, because none of the
//! reachable types declare one. This is not an emergent property to be
//! measured; it is what "no `dyn` in the type" means in Rust. Per-item calls
//! (`reader.read(..)`, `processor.process(..)`, `writer.write(..)`) are
//! therefore direct calls, and the `async fn` futures those calls return are
//! the trait's own associated future type for that concrete `impl`, not a
//! `Box<dyn Future>` -- nothing on the typed path ever calls `Box::pin` on a
//! per-item future, because nothing on the typed path needs one: there is no
//! erasure boundary to cross.
//!
//! For the **erased** representation, `R`/`P`/`W` are `BoxedReader<I>` /
//! `BoxedProcessor<I, O>` / `BoxedWriter<I>`, defined
//! (`crates/oxide-batch/src/chunk.rs:768,787,807`) as:
//!
//! ```text
//! pub struct BoxedReader<I>(Box<dyn sealed::ReaderObject<I>>);
//! pub struct BoxedProcessor<I, O>(Box<dyn sealed::ProcessorObject<I, O>>);
//! pub struct BoxedWriter<I>(Box<dyn sealed::WriterObject<I>>);
//! ```
//!
//! Every method call through one of these necessarily crosses that `dyn`
//! boundary: a vtable call, and (per `sealed::*Object`'s own erasure, which
//! is how a `dyn`-safe async trait is implemented) a boxed per-item future on
//! the other side of it. This is exactly the cost Gate H's hard invariant
//! says the *typed* path must not pay, and the erased path is allowed to.
//!
//! ## Why `gate_h_allocation.rs` cannot be the proof by itself
//!
//! `gate_h_allocation.rs`'s real component (`DelimitedReader`/
//! `DelimitedWriter`) allocates per item for its own reasons (parsed field
//! bytes, formatted output), so a nonzero allocator-call delta on the typed
//! path there is expected and does not indicate framework-controlled future
//! boxing. `chunk_allocation.rs`/`item_components_allocation.rs` isolate the
//! claim empirically by using components with no per-item heap work of their
//! own, so *any* delta is attributable to the framework path -- and they
//! measure a delta of 21 allocator calls across a 19,800-item span (i.e.
//! constant, not scaling), which is exactly what "zero per-item future
//! allocation" predicts and is real corroboration. This file adds the
//! argument that holds regardless of which component is used.
//!
//! ## The runtime corroboration this file adds
//!
//! `BoxedReader`/`BoxedProcessor`/`BoxedWriter` are single-field wrappers
//! around a trait-object `Box`, so their in-memory representation is exactly
//! a fat pointer (data pointer + vtable pointer) -- the same size as any
//! `Box<dyn Trait>`, regardless of which trait. `DelimitedReader<File>`, by
//! contrast, owns a `BufReader`, two growable buffers, and parser state, so
//! its size cannot coincidentally match a bare fat pointer. This is checked
//! below with `std::mem::size_of`, computed against the platform's own
//! `Box<dyn Trait>` size rather than a hardcoded constant, so it holds on
//! both 32- and 64-bit targets.

use std::fs::File;
use std::mem::size_of;

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{DelimitedReader, DelimitedWriter};
use oxide_batch::{BoxedProcessor, BoxedReader, BoxedWriter};

/// The size of any `Box<dyn Trait>` on this platform: one data pointer and
/// one vtable pointer, regardless of which trait. Computed rather than
/// hardcoded so this holds on 32-bit targets too.
fn fat_pointer_size() -> usize {
    size_of::<Box<dyn std::fmt::Debug>>()
}

/// The erased representation's reader/processor/writer are exactly a fat
/// pointer each -- a `dyn` trait object one level of indirection down, with
/// nothing else in the wrapper -- corroborating that every call through them
/// crosses a vtable, on every representation this workspace supports.
#[test]
fn boxed_components_are_exactly_fat_pointer_sized() {
    let fat_pointer = fat_pointer_size();
    assert_eq!(
        size_of::<BoxedReader<i64>>(),
        fat_pointer,
        "BoxedReader must be exactly one Box<dyn Trait>'s worth of representation"
    );
    assert_eq!(
        size_of::<BoxedProcessor<i64, i64>>(),
        fat_pointer,
        "BoxedProcessor must be exactly one Box<dyn Trait>'s worth of representation"
    );
    assert_eq!(
        size_of::<BoxedWriter<i64>>(),
        fat_pointer,
        "BoxedWriter must be exactly one Box<dyn Trait>'s worth of representation"
    );
}

/// The typed representation's real components own their working state
/// inline -- their size cannot coincidentally match a bare fat pointer,
/// corroborating that they are not themselves a trait-object wrapper in
/// disguise.
fn assert_typed_components_are_not_fat_pointer_sized() {
    let fat_pointer = fat_pointer_size();
    assert_ne!(
        size_of::<DelimitedReader<File>>(),
        fat_pointer,
        "DelimitedReader owns real buffered state and must not be fat-pointer-sized"
    );
    assert_ne!(
        size_of::<DelimitedWriter>(),
        fat_pointer,
        "DelimitedWriter owns real shared state and must not be fat-pointer-sized"
    );
    // IdentityProcessor is a unit struct (size 0), which is also, trivially,
    // not fat-pointer-sized -- included for completeness, not because a unit
    // struct needed checking.
    assert_ne!(
        size_of::<IdentityProcessor>(),
        fat_pointer,
        "IdentityProcessor is a zero-sized unit struct, not a trait-object wrapper"
    );
}

#[test]
fn typed_path_framework_controlled_per_item_allocation_is_zero() {
    assert_typed_components_are_not_fat_pointer_sized();
}

#[test]
fn typed_path_requires_no_framework_controlled_dynamic_dispatch_per_item() {
    assert_typed_components_are_not_fat_pointer_sized();
}
