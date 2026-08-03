//! Compiler fixture for the dyn-compatibility comparator. This file must fail
//! to compile, and it is never part of the crate's own build.
//!
//! It reproduces the contract's trait shape standalone so the comparator
//! measures the trait form itself, not anything the spike crate adds. The
//! rejection here is the reason `contract::Boxed*` is built on a sealed
//! dyn-compatible mirror instead of on the contract trait directly.

use std::future::Future;

pub trait ItemReader<I>: Send {
    fn read<'a>(&'a mut self) -> impl Future<Output = Option<I>> + Send + 'a;
}

pub fn erase<I>(reader: &mut dyn ItemReader<I>) -> &mut dyn ItemReader<I> {
    reader
}
