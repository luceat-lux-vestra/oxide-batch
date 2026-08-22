//! The standard item composition catalog (#146).
//!
//! Every component here is a plain, monomorphized [`crate::ItemReader`],
//! [`crate::ItemProcessor`], or [`crate::ItemWriter`] implementation under
//! the accepted ADR-0008 contract: no second component trait hierarchy, no
//! parallel execution engine, and no per-item boxing beyond what an explicit
//! [`crate::BoxedReader`]/[`crate::BoxedProcessor`]/[`crate::BoxedWriter`]
//! handle already costs at construction. Composition and decoration are
//! ordinary generic composition -- a decorator holds its delegate by value
//! and implements the same public trait around it -- so the driving
//! [`crate::ChunkStep`] is unchanged and unaware that decoration occurred.
//!
//! See each submodule for its family's composition-capability discussion:
//! every wrapper here advertises the meet (intersection) of its delegates'
//! capabilities, never a capability none of them has, per
//! [the composition taxonomy](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/architecture/item-processing-model.md#composition-taxonomy).
//!
//! # Families
//!
//! - [`basic`]: iterator/list-backed readers and minimal delegates
//!   ([`IterReader`], [`IdentityProcessor`], [`NoopWriter`]).
//! - [`composite`]: reader/processor/writer composition and delegation
//!   ([`CompositeReader`], [`ChainProcessor`], [`FanOutWriter`]).
//! - [`classify`]: runtime delegate selection from a bounded, configured set
//!   ([`ClassifyingProcessor`], [`ClassifyingWriter`]).
//! - [`validate`]: typed validation failure
//!   ([`ValidatingProcessor`]).
//! - [`filter`]: [`crate::ProcessOutcome::Filtered`]-based filtering
//!   ([`FilterProcessor`]).
//! - [`peek`]: lookahead without corrupting order or progress
//!   ([`PeekReader`]).
//! - [`aggregate`]: bounded aggregation of input items
//!   ([`AggregatingReader`]).
//! - [`sync`]: synchronization/thread-safety wrappers
//!   ([`SynchronizedProcessor`], [`SynchronizedWriter`]).

pub mod aggregate;
pub mod basic;
pub mod classify;
pub mod composite;
pub mod filter;
pub mod peek;
pub mod sync;
pub mod validate;

pub use aggregate::AggregatingReader;
pub use basic::{IdentityProcessor, IterReader, NoopWriter};
pub use classify::{Classifier, ClassifyingProcessor, ClassifyingWriter};
pub use composite::{ChainProcessor, CompositeReader, FanOutWriter};
pub use filter::{FilterProcessor, ItemFilter};
pub use peek::{PeekOutcome, PeekReader};
pub use sync::{SynchronizedProcessor, SynchronizedWriter};
pub use validate::{ItemValidator, ValidatingProcessor};
