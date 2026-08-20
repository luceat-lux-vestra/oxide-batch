//! ADR-0008: the `Send` bound the contract declares is still enforced
//! against a plain `async fn` body. An implementor cannot hold a non-`Send`
//! value across an `.await` and satisfy `ItemReader` — the compiler, not
//! just the spike, has to catch this.

use std::rc::Rc;

use oxide_batch::{ItemReader, ReadContext, ReadOutcome, ReaderError};

async fn flush() {}

struct Counting(u32);

impl ItemReader<u32> for Counting {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u32>, ReaderError> {
        let handle = Rc::new(self.0);
        flush().await;
        Ok(ReadOutcome::Item(*handle))
    }
}

fn main() {}
