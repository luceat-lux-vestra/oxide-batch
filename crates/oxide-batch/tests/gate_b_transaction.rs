//! Gate-B transaction-port compatibility and redaction integration tests.
//!
//! These tests preserve legacy business-only transaction behavior while exercising
//! the additive Gate-B writer-resource boundary and value-redacted diagnostics.

#![allow(
    clippy::expect_used,
    reason = "the fixtures exercise statically constructed transaction-port contracts"
)]

use oxide_batch::{
    BoxFuture, BusinessStatement, BusinessTransaction, BusinessTransactionError,
    BusinessWriteResult, ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress, ChunkTransaction,
    ChunkTransactionError, ChunkWriterResources, GateBEffectIntent, GateBEnlistment,
    GateBEnlistmentError, GateBEnlistmentOutcome, GateBInboxGuard, GateBInboxGuardOutcome,
    GateBOutboxObligation, StopSource, WriteContext,
};

struct BusinessFixture;

impl BusinessTransaction for BusinessFixture {
    fn execute<'a>(
        &'a mut self,
        _statement: BusinessStatement<'a>,
    ) -> BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>> {
        Box::pin(async { Err(BusinessTransactionError::Rejected) })
    }
}

#[derive(Default)]
struct GateBFixture {
    inbox_guards: usize,
}

impl GateBEnlistment for GateBFixture {
    fn guard_inbox<'a>(
        &'a mut self,
        guard: GateBInboxGuard<'a>,
    ) -> BoxFuture<'a, Result<GateBInboxGuardOutcome, GateBEnlistmentError>> {
        self.inbox_guards += 1;
        assert_eq!(guard.inbound_identity(), b"inbound-1");
        assert_eq!(guard.request_identity(), b"request-1");
        Box::pin(async { Ok(GateBInboxGuardOutcome::FirstDelivery) })
    }

    fn ensure_effect_intent<'a>(
        &'a mut self,
        _intent: GateBEffectIntent<'a>,
    ) -> BoxFuture<'a, Result<GateBEnlistmentOutcome, GateBEnlistmentError>> {
        Box::pin(async { Ok(GateBEnlistmentOutcome::CreatedOrMatching) })
    }

    fn enqueue_outbox<'a>(
        &'a mut self,
        _obligation: GateBOutboxObligation<'a>,
    ) -> BoxFuture<'a, Result<GateBEnlistmentOutcome, GateBEnlistmentError>> {
        Box::pin(async { Ok(GateBEnlistmentOutcome::CreatedOrMatching) })
    }
}

struct BusinessOnlyTransaction {
    business: BusinessFixture,
}

impl ChunkTransaction for BusinessOnlyTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        Some(&mut self.business)
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

struct CombinedTransaction {
    business: BusinessFixture,
    gate_b: GateBFixture,
}

impl ChunkTransaction for CombinedTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        Some(&mut self.business)
    }

    fn gate_b_enlistment(&mut self) -> Option<&mut dyn GateBEnlistment> {
        Some(&mut self.gate_b)
    }

    fn writer_resources(&mut self) -> ChunkWriterResources<'_> {
        ChunkWriterResources::new(Some(&mut self.business), Some(&mut self.gate_b))
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn existing_chunk_transactions_default_to_no_gate_b_capability() {
    let mut transaction = BusinessOnlyTransaction {
        business: BusinessFixture,
    };

    assert_eq!(
        transaction.require_gate_b_enlistment().map(|_| ()),
        Err(GateBEnlistmentError::Unsupported)
    );

    let resources = transaction.writer_resources();
    assert!(resources.is_enlisted());
    assert!(!resources.has_gate_b_enlistment());
}

#[test]
fn legacy_write_context_enlisted_constructor_remains_business_only() {
    let (_source, stop) = StopSource::new();
    let mut business = BusinessFixture;
    let mut context = WriteContext::enlisted(&stop, &mut business);

    assert!(context.is_enlisted());
    assert!(context.transaction().is_some());
    assert!(!context.has_gate_b_enlistment());
    assert_eq!(
        context.require_gate_b_enlistment().map(|_| ()),
        Err(GateBEnlistmentError::Unsupported)
    );
}

#[test]
fn combined_writer_resources_reborrow_business_and_gate_b_sequentially() {
    let (_source, stop) = StopSource::new();
    let mut transaction = CombinedTransaction {
        business: BusinessFixture,
        gate_b: GateBFixture::default(),
    };

    {
        let resources = transaction.writer_resources();
        let mut context = WriteContext::with_resources(&stop, resources);

        assert!(context.is_enlisted());
        assert!(context.has_gate_b_enlistment());
        assert!(context.transaction().is_some());

        let enlistment = context
            .require_gate_b_enlistment()
            .expect("combined transaction exposes Gate-B enlistment");
        let outcome = futures_executor::block_on(
            enlistment.guard_inbox(GateBInboxGuard::new(b"inbound-1", b"request-1")),
        )
        .expect("fixture guard succeeds");
        assert_eq!(outcome, GateBInboxGuardOutcome::FirstDelivery);
    }

    assert_eq!(transaction.gate_b.inbox_guards, 1);
}

#[test]
fn gate_b_runtime_identity_debug_output_is_value_redacted() {
    let guard = GateBInboxGuard::new(b"secret-inbound", b"secret-request");
    let debug = format!("{guard:?}");

    assert!(debug.contains("inbound_identity_bytes"));
    assert!(debug.contains("request_identity_bytes"));
    assert!(!debug.contains("secret-inbound"));
    assert!(!debug.contains("secret-request"));
}
