//! Compiler fixture: what an implementer sees when a component body holds a
//! non-`Send` value across an await. This file must fail to compile.
//!
//! The contract's `Send` bound is the one thing a plain `async fn` impl cannot
//! state for itself, so it matters that the compiler still enforces it and
//! still points at the offending value.

use std::future::Future;
use std::rc::Rc;

pub struct ReadContext<'a>(&'a ());

pub trait ItemReader<I>: Send {
    fn read<'a>(&'a mut self, context: ReadContext<'a>)
    -> impl Future<Output = Option<I>> + Send + 'a;
}

async fn flush() {}

pub struct Counting(u32);

impl ItemReader<u32> for Counting {
    async fn read(&mut self, _context: ReadContext<'_>) -> Option<u32> {
        let handle = Rc::new(self.0);
        flush().await;
        Some(*handle)
    }
}
