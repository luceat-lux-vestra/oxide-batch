//! Sensible default restart-relevant revisions for test fixtures.
//!
//! These compose only public [`ChunkComponentRevisions`]/[`ChunkRestartContract`]
//! constructors with fixed, always-valid literals; they never touch a
//! production internal type. Using them keeps a component/job test's
//! restart-relevant identity boilerplate out of the test body, exactly as
//! [`crate::TestStep`] and [`crate::TestJob`] intend.

use oxide_batch::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ComponentRevision,
    StateSchemaId, StateSchemaVersion,
};

/// Builds component revisions and an `AtLeastOnce` restart contract from
/// fixed literals, suitable whenever a test does not care about restart
/// compatibility edges.
///
/// # Panics
///
/// Never in practice: every constructed value is a fixed literal already
/// known to satisfy the underlying domain validation.
#[must_use]
pub fn default_chunk_component_revisions() -> ChunkComponentRevisions {
    chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtLeastOnce)
}

/// Builds component revisions and a restart contract under an explicit
/// delivery mode from fixed literals.
///
/// # Panics
///
/// Never in practice: every constructed value is a fixed literal already
/// known to satisfy the underlying domain validation.
#[must_use]
#[allow(
    clippy::unwrap_used,
    reason = "fixed literal revisions/schema identities cannot fail validation"
)]
pub fn chunk_component_revisions_with_delivery_mode(
    delivery_mode: ChunkDeliveryMode,
) -> ChunkComponentRevisions {
    let revision = |value: &str| ComponentRevision::new(value).unwrap();
    ChunkComponentRevisions::new(
        revision("oxide-batch-test.reader-v1"),
        revision("oxide-batch-test.processor-v1"),
        revision("oxide-batch-test.writer-v1"),
        revision("oxide-batch-test.checkpoint-v1"),
        ChunkRestartContract::new(
            StateSchemaId::new("oxide-batch-test.checkpoint").unwrap(),
            StateSchemaVersion::new(1).unwrap(),
            StateSchemaId::new("oxide-batch-test.context").unwrap(),
            StateSchemaVersion::new(1).unwrap(),
            delivery_mode,
        ),
    )
}
