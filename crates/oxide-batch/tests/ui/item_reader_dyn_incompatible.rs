//! ADR-0008: the public item component contract is not meant to be named as
//! `dyn Trait`. `BoxedReader` is the supported erasure mechanism instead.

fn erase<I>(reader: &mut dyn oxide_batch::ItemReader<I>) -> &mut dyn oxide_batch::ItemReader<I> {
    reader
}

fn main() {}
