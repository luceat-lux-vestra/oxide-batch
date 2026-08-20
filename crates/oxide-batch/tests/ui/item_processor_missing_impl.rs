//! ADR-0008: a component that does not implement `ItemProcessor<I, O>` is
//! rejected with the contract's own diagnostic wording, naming the component,
//! the item types, and the signature to write.

struct NotAProcessor;

fn drive<I, O, P: oxide_batch::ItemProcessor<I, O>>(_processor: P) {}

fn main() {
    drive::<u64, String, _>(NotAProcessor);
}
