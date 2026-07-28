#![allow(async_fn_in_trait)]

pub trait NativeProcessor {
    async fn process(&self, item: &str) -> String;
}

pub fn erase(processor: &dyn NativeProcessor) -> &dyn NativeProcessor {
    processor
}
