//! The standard item composition catalog (#146), plus restartable flat-file
//! I/O (#147).
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
//! - [`delimited`]: restartable delimited/CSV file I/O (#147, `IO-FLAT-001`)
//!   ([`DelimitedReader`], [`DelimitedWriter`]).
//! - [`fixed_width`]: restartable fixed-width file I/O (#147, `IO-FLAT-001`)
//!   ([`FixedWidthReader`], [`FixedWidthWriter`]).
//! - [`jsonl`]: restartable JSON Lines file I/O (#148, `IO-STRUCTURED-001` M6
//!   slice) ([`JsonLinesReader`], [`JsonLinesWriter`]).
//! - [`json_array`]: restartable streaming top-level JSON-array file I/O
//!   (#148, `IO-STRUCTURED-001` M6 slice) ([`JsonArrayReader`],
//!   [`JsonArrayWriter`]).
//! - [`postgres_cursor`], [`postgres_paging`], [`postgres_batch`]: real
//!   `PostgreSQL` server-side cursor streaming, restartable keyset paging,
//!   and bounded same-resource-enlisted SQL batch writing (#149,
//!   `IO-DB-001` M6 `PostgreSQL` slice, `postgres` feature)
//!   ([`PostgresCursorReader`], [`PostgresPagingReader`],
//!   [`PostgresBatchWriter`]).

pub mod aggregate;
pub mod basic;
pub mod classify;
pub mod composite;
pub mod delimited;
pub mod filter;
pub mod fixed_width;
pub mod json_array;
pub mod jsonl;
pub mod multi_resource;
pub mod object_store;
pub mod peek;
#[cfg(feature = "postgres")]
pub mod postgres_batch;
#[cfg(feature = "postgres")]
pub mod postgres_cursor;
#[cfg(feature = "postgres")]
mod postgres_keyset;
#[cfg(feature = "postgres")]
pub mod postgres_paging;
pub mod sync;
pub mod validate;

pub use aggregate::AggregatingReader;
pub use basic::{IdentityProcessor, IterReader, NoopWriter};
pub use classify::{Classifier, ClassifyingProcessor, ClassifyingWriter};
pub use composite::{ChainProcessor, CompositeReader, FanOutWriter};
pub use delimited::{
    DelimitedDialect, DelimitedReader, DelimitedReaderStream, DelimitedRecord, DelimitedTerminator,
    DelimitedWriter, DelimitedWriterStream, delimited_file_reader, delimited_reader,
    delimited_writer,
};
pub use filter::{FilterProcessor, ItemFilter};
pub use fixed_width::{
    FixedWidthField, FixedWidthLayout, FixedWidthReader, FixedWidthReaderStream, FixedWidthRecord,
    FixedWidthTerminator, FixedWidthWriter, FixedWidthWriterStream, fixed_width_file_reader,
    fixed_width_reader, fixed_width_writer,
};
pub use json_array::{
    JsonArrayFormat, JsonArrayReader, JsonArrayReaderStream, JsonArrayWriter,
    JsonArrayWriterStream, json_array_file_reader, json_array_reader, json_array_writer,
};
pub use jsonl::{
    JsonLinesFormat, JsonLinesReader, JsonLinesReaderStream, JsonLinesTerminator, JsonLinesWriter,
    JsonLinesWriterStream, jsonl_file_reader, jsonl_reader, jsonl_writer,
};
pub use multi_resource::{
    BatchCountRollover, MultiResourceConfigError, MultiResourceOpenError, MultiResourceReader,
    MultiResourceReaderOpener, MultiResourceReaderStream, MultiResourceWriter,
    MultiResourceWriterOpener, MultiResourceWriterStream, MultiResourceWriterTriple, NoRollover,
    ResourceIdentity, ResourceSet, ResourceSetRevision, RolloverPolicy, WriterConfigError,
    multi_resource_reader, multi_resource_writer,
};
pub use object_store::{
    InMemoryObjectStore, ObjectIdentity, ObjectItemReader, ObjectItemReaderStream,
    ObjectItemWriter, ObjectItemWriterStream, ObjectListContinuation, ObjectListPage,
    ObjectMetadata, ObjectStoreCapability, ObjectStoreConfigError, ObjectStoreError,
    ObjectStoreReaderOpener, ObjectStoreWriterOpener, ObjectVersionToken,
};
pub use peek::{PeekOutcome, PeekReader};
#[cfg(feature = "postgres")]
pub use postgres_batch::{
    POSTGRESQL_MAX_BIND_PARAMETERS, PostgresBatchMode, PostgresBatchWriter, postgres_batch_writer,
};
#[cfg(feature = "postgres")]
pub use postgres_cursor::{
    PostgresCursorFormat, PostgresCursorReader, PostgresCursorReaderStream, postgres_cursor_reader,
};
#[cfg(feature = "postgres")]
pub use postgres_keyset::{
    KeysetColumn, KeysetColumnKind, PostgresComponentConfigError, PostgresRow,
};
#[cfg(feature = "postgres")]
pub use postgres_paging::{
    PostgresPagingFormat, PostgresPagingReader, PostgresPagingReaderStream, postgres_paging_reader,
};
pub use sync::{SynchronizedProcessor, SynchronizedWriter};
pub use validate::{ItemValidator, ValidatingProcessor};
