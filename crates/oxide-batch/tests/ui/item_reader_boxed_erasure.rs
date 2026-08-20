//! ADR-0008: `BoxedReader::new` is the single, explicit erasure boundary. A
//! `BoxedReader<I>` still implements `ItemReader<I>`, and heterogeneous
//! concrete readers can be stored behind the same handle type.

use oxide_batch::{BoxedReader, ItemReader, ReadContext, ReadOutcome, ReaderError};

struct Counter(u64);

impl ItemReader<u64> for Counter {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        self.0 += 1;
        Ok(ReadOutcome::Item(self.0))
    }
}

struct Empty;

impl ItemReader<u64> for Empty {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        Ok(ReadOutcome::EndOfInput)
    }
}

fn accepts_reader<I, R: ItemReader<I>>(_reader: &R) {}

fn main() {
    let registry: Vec<BoxedReader<u64>> =
        vec![BoxedReader::new(Counter(0)), BoxedReader::new(Empty)];
    for reader in &registry {
        accepts_reader::<u64, _>(reader);
    }
}
