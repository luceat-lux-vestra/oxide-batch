//! The property the superseding ADR has to preserve: transaction borrowing.
//! `WriteContext<'a>` carries `Option<&'a mut dyn BusinessTransaction>`, and
//! `&mut` to a trait object is invariant, so tying the call lifetime is most
//! likely to break here. The gate requires that the new contract not weaken
//! it.

#![allow(clippy::expect_used, clippy::items_after_statements)]

use oxide_batch::{
    BusinessStatement, BusinessTransaction, BusinessTransactionError, BusinessValue,
    BusinessWriteResult, StopSource, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_m6_spikes::composite::FanOutWriter;
use oxide_batch_m6_spikes::contract::{BoxedWriter, ItemWriter};
use oxide_batch_m6_spikes::executor::block_on;
use oxide_batch_m6_spikes::workload::Output;

struct RecordingTransaction {
    statements: u64,
}

impl BusinessTransaction for RecordingTransaction {
    fn execute<'a>(
        &'a mut self,
        _statement: BusinessStatement<'a>,
    ) -> oxide_batch::BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>> {
        Box::pin(async move {
            self.statements += 1;
            Ok(BusinessWriteResult::new(1))
        })
    }
}

/// A writer that borrows the enlisted transaction for the duration of its call.
struct Enlisting;

impl ItemWriter<Output> for Enlisting {
    async fn write(
        &self,
        items: &[Output],
        mut context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        let Some(transaction) = context.transaction() else {
            return Err(WriterError::new());
        };
        for item in items {
            let values = [BusinessValue::i64(i64::try_from(item.id).unwrap_or(0))];
            let statement = BusinessStatement::new("insert into ledger values ($1)", &values);
            transaction
                .execute(statement)
                .await
                .map_err(|_| WriterError::new())?;
        }
        Ok(WriteOutcome::Written)
    }
}

fn batch() -> [Output; 3] {
    [
        Output { id: 1, payload: 1 },
        Output { id: 2, payload: 2 },
        Output { id: 3, payload: 3 },
    ]
}

#[test]
fn a_typed_writer_borrows_the_enlisted_transaction() {
    let (_source, stop) = StopSource::new();
    let mut transaction = RecordingTransaction { statements: 0 };
    let items = batch();

    let outcome =
        block_on(Enlisting.write(&items, WriteContext::enlisted(&stop, &mut transaction)));

    assert_eq!(outcome, Ok(WriteOutcome::Written));
    assert_eq!(transaction.statements, 3);
}

#[test]
fn a_boxed_writer_borrows_the_same_enlisted_transaction() {
    let (_source, stop) = StopSource::new();
    let mut transaction = RecordingTransaction { statements: 0 };
    let items = batch();
    let writer = BoxedWriter::new(Enlisting);

    let outcome = block_on(writer.write(&items, WriteContext::enlisted(&stop, &mut transaction)));

    assert_eq!(outcome, Ok(WriteOutcome::Written));
    assert_eq!(transaction.statements, 3);
}

#[test]
fn a_fan_out_writer_lands_both_delegates_in_one_enlisted_transaction() {
    let (_source, stop) = StopSource::new();
    let mut transaction = RecordingTransaction { statements: 0 };
    let items = batch();

    let writer = FanOutWriter::new(Enlisting, Enlisting);
    let outcome = block_on(writer.write(&items, WriteContext::enlisted(&stop, &mut transaction)));

    assert_eq!(outcome, Ok(WriteOutcome::Written));
    // Two delegates, three items each, reborrowing one transaction in turn.
    assert_eq!(transaction.statements, 6);
}
