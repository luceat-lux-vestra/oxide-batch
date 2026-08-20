//! ADR-0008: implementors write a natural `async fn` with no lifetime, future
//! type, or `Box::pin` in sight, and the impl still satisfies `ChunkStep`.

use oxide_batch::{ChunkSize, ItemReader, ReadContext, ReadOutcome, ReaderError};

struct Counter(u64);

impl ItemReader<u64> for Counter {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        self.0 += 1;
        Ok(ReadOutcome::Item(self.0))
    }
}

fn accepts_reader<I, R: ItemReader<I>>(_reader: &R) {}

fn main() {
    let counter = Counter(0);
    accepts_reader::<u64, _>(&counter);
    let _ = ChunkSize::new(1);
}
