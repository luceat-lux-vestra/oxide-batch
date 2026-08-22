//! A representative #146 decorated pipeline: peek over a composite reader,
//! a filter/identity processor chain, and a synchronized recording writer.
//! Shared by the typed/erased equivalence test and the allocation-regression
//! test so both measure the exact same pipeline shape.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use oxide_batch::item_components::{
    ChainProcessor, CompositeReader, FilterProcessor, IdentityProcessor, IterReader, PeekReader,
    SynchronizedWriter,
};
use oxide_batch::{ItemWriter, WriteContext, WriteOutcome, WriterError};

pub type Reader = PeekReader<i64, CompositeReader<IterReader<std::vec::IntoIter<i64>>>>;
pub type Processor = ChainProcessor<FilterProcessor<i64, fn(&i64) -> bool>, IdentityProcessor, i64>;
pub type Writer = SynchronizedWriter<RecordingWriter>;

/// Every item is kept; the point of this predicate is exercising the filter
/// decorator's dispatch, not actually filtering. Takes `&i64` (rather than
/// `i64`) to match `ItemFilter`'s borrowed-item signature.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn keep_all(_item: &i64) -> bool {
    true
}

pub struct RecordingWriter(pub Arc<Mutex<Vec<i64>>>);

impl ItemWriter<i64> for RecordingWriter {
    async fn write(
        &self,
        items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

/// Splits `0..items` across two `IterReader` delegates, concatenated by
/// `CompositeReader`, and wraps the result in `PeekReader`.
#[must_use]
pub fn reader(items: u32) -> Reader {
    let half = i64::from(items / 2);
    let total = i64::from(items);
    let first: Vec<i64> = (0..half).collect();
    let second: Vec<i64> = (half..total).collect();
    PeekReader::new(CompositeReader::new(vec![
        IterReader::new(first),
        IterReader::new(second),
    ]))
}

#[must_use]
pub fn processor() -> Processor {
    ChainProcessor::new(
        FilterProcessor::new(keep_all as fn(&i64) -> bool),
        IdentityProcessor,
    )
}

#[must_use]
pub fn writer(output: Arc<Mutex<Vec<i64>>>) -> Writer {
    SynchronizedWriter::new(RecordingWriter(output))
}
