//! #167 deterministic writer-checkpoint coherence regression evidence.
//!
//! The historical flat-file writers used one lock for the physical file and a
//! second lock for an additive committed-byte counter. The negative controls
//! below force the exact harmful interleaving without sleeps: the first
//! physical write lands, the second write lands and publishes its increment,
//! and an update-equivalent snapshot reads the counter before the first
//! increment is published. The snapshot can therefore describe no physical
//! write-call prefix even though the final additive total is numerically
//! correct.
//!
//! The production tests then race two unequal-sized public `write()` calls
//! against the real `ItemStream::update()` API. The fixed single-state-lock
//! implementation is linearizable: the observed checkpoint is always either
//! the previous committed boundary, the first complete physical write-call
//! boundary, or the final boundary. The captured checkpoint is then fed back
//! through a fresh stream's `open()` to prove restart truncation preserves an
//! exact complete write prefix.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Barrier, Mutex, PoisonError, mpsc};
use std::thread;

use oxide_batch::item_components::{
    DelimitedDialect, DelimitedRecord, FixedWidthField, FixedWidthLayout, FixedWidthRecord,
    delimited_writer, fixed_width_writer,
};
use oxide_batch::{
    ComponentStateEnvelope, ComponentStatePayload, ComponentStreamIdentity, ItemStream, ItemWriter,
    StopSource, StreamOpenContext, StreamUpdateContext, WriteContext,
};

fn temp_path(name: &str, extension: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos();
    std::env::temp_dir().join(format!("oxide-batch-167-{name}-{nonce}.{extension}"))
}

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(format!("oxide-batch-167.{name}")).expect("test identity is valid")
}

fn committed_byte(envelope: &ComponentStateEnvelope) -> u64 {
    let ComponentStatePayload::Inline(payload) = envelope.payload().unwrap() else {
        panic!("writer checkpoints must use an inline payload");
    };
    serde_json::from_slice::<serde_json::Value>(&payload)
        .unwrap()
        .get("committed_bytes")
        .and_then(serde_json::Value::as_u64)
        .expect("writer checkpoint committed_bytes")
}

fn byte_index(offset: u64) -> usize {
    usize::try_from(offset).expect("test byte offset must fit usize")
}

/// Deterministic negative control for the pre-#167 split-lock algorithm.
///
/// `first_bytes` is physically appended first but its additive checkpoint
/// publication is paused. `second_bytes` then appends and publishes first.
/// The returned snapshot is exactly what the old stream `update()` could see
/// in that window.
fn legacy_split_lock_intermediate_checkpoint(
    path: &std::path::Path,
    first_bytes: &'static [u8],
    second_bytes: &'static [u8],
) -> (u64, Vec<u8>, u64) {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let file = Arc::new(Mutex::new(file));
    let committed_bytes = Arc::new(Mutex::new(0_u64));

    let (first_written_tx, first_written_rx) = mpsc::sync_channel::<()>(0);
    let (resume_first_tx, resume_first_rx) = mpsc::sync_channel::<()>(0);

    let file_for_first = Arc::clone(&file);
    let committed_for_first = Arc::clone(&committed_bytes);
    let first = thread::spawn(move || {
        {
            let mut file = file_for_first
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            file.write_all(first_bytes).unwrap();
            file.sync_data().unwrap();
        }
        first_written_tx.send(()).unwrap();
        resume_first_rx.recv().unwrap();
        let mut committed = committed_for_first
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *committed = committed.saturating_add(first_bytes.len() as u64);
    });

    first_written_rx.recv().unwrap();

    let file_for_second = Arc::clone(&file);
    let committed_for_second = Arc::clone(&committed_bytes);
    let second = thread::spawn(move || {
        {
            let mut file = file_for_second
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            file.write_all(second_bytes).unwrap();
            file.sync_data().unwrap();
        }
        let mut committed = committed_for_second
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *committed = committed.saturating_add(second_bytes.len() as u64);
    });
    second.join().unwrap();

    let intermediate = *committed_bytes
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let physical = std::fs::read(path).unwrap();

    resume_first_tx.send(()).unwrap();
    first.join().unwrap();

    let final_committed = *committed_bytes
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    (intermediate, physical, final_committed)
}

#[test]
fn legacy_delimited_split_lock_can_publish_a_mid_record_checkpoint() {
    let path = temp_path("legacy-delimited", "csv");
    let (intermediate, physical, final_committed) =
        legacy_split_lock_intermediate_checkpoint(&path, b"AAAA\n", b"B\n");

    assert_eq!(physical, b"AAAA\nB\n");
    assert_eq!(intermediate, 2);
    assert!(
        ![0_u64, 5, 7].contains(&intermediate),
        "the historical intermediate checkpoint must not describe any complete physical write-call prefix"
    );
    assert_eq!(
        &physical[..byte_index(intermediate)],
        b"AA",
        "restart at the historical snapshot would retain only part of the first CSV record"
    );
    assert_eq!(final_committed, physical.len() as u64);

    let _ = std::fs::remove_file(path);
}

#[test]
fn legacy_fixed_width_split_lock_can_publish_the_wrong_write_prefix() {
    let path = temp_path("legacy-fixed-width", "txt");
    let (intermediate, physical, final_committed) =
        legacy_split_lock_intermediate_checkpoint(&path, b"1\n2\n3\n", b"4\n");

    assert_eq!(physical, b"1\n2\n3\n4\n");
    assert_eq!(intermediate, 2);
    assert!(
        ![0_u64, 6, 8].contains(&intermediate),
        "the historical snapshot is a record boundary by accident, but not a complete physical write-call prefix"
    );
    assert_eq!(
        &physical[..byte_index(intermediate)],
        b"1\n",
        "restart would keep only one record from the first three-record write while discarding the rest and the later successful write"
    );
    assert_eq!(final_committed, physical.len() as u64);

    let _ = std::fs::remove_file(path);
}

#[test]
fn delimited_update_racing_unequal_writes_observes_only_complete_write_prefixes() {
    let path = temp_path("delimited-linearizable", "csv");
    let namespace = identity("delimited-linearizable");
    let (writer, stream, _contract) =
        delimited_writer(&path, DelimitedDialect::csv(), namespace.clone()).unwrap();
    let (_open_source, open_stop) = StopSource::new();
    futures_executor::block_on(stream.open(StreamOpenContext::new(None, &open_stop))).unwrap();

    let writer = Arc::new(writer);
    let stream = Arc::new(stream);
    let start = Arc::new(Barrier::new(4));

    let writer_a = Arc::clone(&writer);
    let start_a = Arc::clone(&start);
    let handle_a = thread::spawn(move || {
        let (_source, stop) = StopSource::new();
        let items = [DelimitedRecord::new(vec!["AAAA".to_owned()])];
        start_a.wait();
        futures_executor::block_on(writer_a.write(&items, WriteContext::non_transactional(&stop)))
            .unwrap();
    });

    let writer_b = Arc::clone(&writer);
    let start_b = Arc::clone(&start);
    let handle_b = thread::spawn(move || {
        let (_source, stop) = StopSource::new();
        let items = [DelimitedRecord::new(vec!["B".to_owned()])];
        start_b.wait();
        futures_executor::block_on(writer_b.write(&items, WriteContext::non_transactional(&stop)))
            .unwrap();
    });

    let stream_for_update = Arc::clone(&stream);
    let start_update = Arc::clone(&start);
    let update = thread::spawn(move || {
        let (_source, stop) = StopSource::new();
        start_update.wait();
        futures_executor::block_on(stream_for_update.update(StreamUpdateContext::new(&stop)))
            .unwrap()
    });

    start.wait();
    let envelope = update.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();

    let final_bytes = std::fs::read(&path).unwrap();
    let first_boundary = if final_bytes == b"AAAA\nB\n" {
        5_u64
    } else if final_bytes == b"B\nAAAA\n" {
        2_u64
    } else {
        panic!(
            "both public writes must land exactly once as whole serialized batches: {final_bytes:?}"
        );
    };

    let observed = committed_byte(&envelope);
    assert!(
        [0_u64, first_boundary, final_bytes.len() as u64].contains(&observed),
        "a concurrent update may observe before, between, or after serialized writes, but never inside one write transition: observed={observed}, final={final_bytes:?}"
    );

    let (_source, stop) = StopSource::new();
    let final_envelope =
        futures_executor::block_on(stream.update(StreamUpdateContext::new(&stop))).unwrap();
    assert_eq!(committed_byte(&final_envelope), final_bytes.len() as u64);

    drop(writer);
    drop(stream);

    let (_restart_writer, restart_stream, _contract) =
        delimited_writer(&path, DelimitedDialect::csv(), namespace).unwrap();
    let (_restart_source, restart_stop) = StopSource::new();
    futures_executor::block_on(
        restart_stream.open(StreamOpenContext::new(Some(&envelope), &restart_stop)),
    )
    .unwrap();

    let restarted = std::fs::read(&path).unwrap();
    assert_eq!(restarted, final_bytes[..byte_index(observed)]);
    assert!(
        [
            b"".as_slice(),
            b"AAAA\n".as_slice(),
            b"B\n".as_slice(),
            b"AAAA\nB\n".as_slice(),
            b"B\nAAAA\n".as_slice(),
        ]
        .contains(&restarted.as_slice()),
        "restart must preserve a complete CSV write prefix, never a partial record"
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn fixed_width_update_racing_unequal_batches_observes_only_complete_write_prefixes() {
    let path = temp_path("fixed-width-linearizable", "txt");
    let namespace = identity("fixed-width-linearizable");
    let layout = FixedWidthLayout::new(vec![FixedWidthField::new(1)]);
    let (writer, stream, _contract) =
        fixed_width_writer(&path, layout.clone(), namespace.clone()).unwrap();
    let (_open_source, open_stop) = StopSource::new();
    futures_executor::block_on(stream.open(StreamOpenContext::new(None, &open_stop))).unwrap();

    let writer = Arc::new(writer);
    let stream = Arc::new(stream);
    let start = Arc::new(Barrier::new(4));

    let writer_a = Arc::clone(&writer);
    let start_a = Arc::clone(&start);
    let handle_a = thread::spawn(move || {
        let (_source, stop) = StopSource::new();
        let items = [
            FixedWidthRecord::new(vec!["1".to_owned()]),
            FixedWidthRecord::new(vec!["2".to_owned()]),
            FixedWidthRecord::new(vec!["3".to_owned()]),
        ];
        start_a.wait();
        futures_executor::block_on(writer_a.write(&items, WriteContext::non_transactional(&stop)))
            .unwrap();
    });

    let writer_b = Arc::clone(&writer);
    let start_b = Arc::clone(&start);
    let handle_b = thread::spawn(move || {
        let (_source, stop) = StopSource::new();
        let items = [FixedWidthRecord::new(vec!["4".to_owned()])];
        start_b.wait();
        futures_executor::block_on(writer_b.write(&items, WriteContext::non_transactional(&stop)))
            .unwrap();
    });

    let stream_for_update = Arc::clone(&stream);
    let start_update = Arc::clone(&start);
    let update = thread::spawn(move || {
        let (_source, stop) = StopSource::new();
        start_update.wait();
        futures_executor::block_on(stream_for_update.update(StreamUpdateContext::new(&stop)))
            .unwrap()
    });

    start.wait();
    let envelope = update.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();

    let final_bytes = std::fs::read(&path).unwrap();
    let first_boundary = if final_bytes == b"1\n2\n3\n4\n" {
        6_u64
    } else if final_bytes == b"4\n1\n2\n3\n" {
        2_u64
    } else {
        panic!(
            "both public fixed-width writes must land exactly once as whole serialized batches: {final_bytes:?}"
        );
    };

    let observed = committed_byte(&envelope);
    assert!(
        [0_u64, first_boundary, final_bytes.len() as u64].contains(&observed),
        "a concurrent update may observe only complete serialized write-call prefixes: observed={observed}, final={final_bytes:?}"
    );

    let (_source, stop) = StopSource::new();
    let final_envelope =
        futures_executor::block_on(stream.update(StreamUpdateContext::new(&stop))).unwrap();
    assert_eq!(committed_byte(&final_envelope), final_bytes.len() as u64);

    drop(writer);
    drop(stream);

    let (_restart_writer, restart_stream, _contract) =
        fixed_width_writer(&path, layout, namespace).unwrap();
    let (_restart_source, restart_stop) = StopSource::new();
    futures_executor::block_on(
        restart_stream.open(StreamOpenContext::new(Some(&envelope), &restart_stop)),
    )
    .unwrap();

    let restarted = std::fs::read(&path).unwrap();
    assert_eq!(restarted, final_bytes[..byte_index(observed)]);
    assert_eq!(
        restarted.len() % 2,
        0,
        "restart checkpoint must remain on a complete fixed-width record boundary"
    );

    let _ = std::fs::remove_file(path);
}
