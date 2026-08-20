//! ADR-0008: the item type is not constrained to `'static`, by the contract
//! or by `BoxedReader`. Only the *component* placed into a handle must be
//! `'static` — the ordinary requirement for storing it in a registry — and
//! that bound does not propagate to the item type it reads.

use oxide_batch::{BoxedReader, ItemReader, ReadContext, ReadOutcome, ReaderError};

/// A reader whose item type borrows from a buffer it owns for its own
/// lifetime, not from any particular call. `&'buf str` is not `'static`, and
/// the contract accepts it without complaint.
struct BorrowingReader<'buf> {
    items: &'buf [&'buf str],
    next: usize,
}

impl<'buf> ItemReader<&'buf str> for BorrowingReader<'buf> {
    async fn read(
        &mut self,
        _context: ReadContext<'_>,
    ) -> Result<ReadOutcome<&'buf str>, ReaderError> {
        let item = self.items.get(self.next).copied();
        self.next += 1;
        Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

fn accepts_reader<I, R: ItemReader<I>>(_reader: &R) {}

// `BoxedReader<I>` carries no `I: 'static` bound of its own: this is a
// well-formed type for an arbitrary, non-`'static` item lifetime `'a`. Only
// `BoxedReader::new`'s own `R: 'static` bound constrains the *component*.
fn accepts_boxed_handle(_handle: BoxedReader<&str>) {}

fn main() {
    let words = ["first", "second"];
    let reader = BorrowingReader {
        items: &words,
        next: 0,
    };
    accepts_reader::<&str, _>(&reader);
    let _ = accepts_boxed_handle;
}
