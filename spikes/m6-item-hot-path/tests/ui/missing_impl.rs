//! Compiler fixture: what an implementer sees when a component does not
//! satisfy the contract. This file must fail to compile.
//!
//! It reproduces the contract's declaration and its
//! `#[diagnostic::on_unimplemented]` attribute standalone, so the comparator
//! measures the wording the attribute produces rather than anything else in
//! the crate.

use std::future::Future;

pub struct ReadContext<'a>(&'a ());

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item reader for `{I}`",
    label = "this component cannot read `{I}`",
    note = "implement `ItemReader<{I}>` with `async fn read(&mut self, context: ReadContext<'_>)`",
    note = "the returned future must be `Send`: do not hold a non-`Send` value across an await"
)]
pub trait ItemReader<I>: Send {
    fn read<'a>(&'a mut self, context: ReadContext<'a>)
    -> impl Future<Output = Option<I>> + Send + 'a;
}

pub struct Invoice;
pub struct NotAReader;

pub fn drive<I, R: ItemReader<I>>(_reader: R) {}

pub fn misuse() {
    drive::<Invoice, _>(NotAReader);
}
