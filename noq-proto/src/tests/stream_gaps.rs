//! One stream filling a 16 MiB receive window, delivered in a chosen order to a reader
//! that reads eagerly or not at all. The assembler's chunk bound must count real gaps,
//! not unread frames.

use alloc::{sync::Arc, vec, vec::Vec};

use super::util::{Inbound, Pair, client_config, server_config};
use crate::{
    ConnectionError, Dir, Duration, Event, ReadError, StreamEvent, TransportConfig,
    TransportErrorCode, VarInt, congestion::NewRenoConfig,
};

const STREAM_WINDOW: u32 = 16 << 20;
const CONN_WINDOW: u32 = 64 << 20;

#[derive(Clone, Copy, Debug)]
enum Delivery {
    /// Delivered exactly as sent.
    InOrder,
    /// Datagram `index` is lost and never retransmitted.
    DropOne { index: usize },
    /// Every odd datagram among the first `2 * holes` is lost, leaving `holes` gaps that
    /// are never repaired; everything after them arrives in order.
    OddLostThenInOrder { holes: usize },
}

#[derive(Clone, Copy, Debug)]
enum Reader {
    /// Reads everything readable after every delivery batch.
    Eager,
    /// Reads nothing until every datagram has been delivered.
    Stalled,
}

struct Outcome {
    lost: Option<ConnectionError>,
    read: u64,
}

fn transport() -> TransportConfig {
    let mut t = TransportConfig::default();
    t.stream_receive_window(VarInt::from_u32(STREAM_WINDOW))
        .receive_window(VarInt::from_u32(CONN_WINDOW))
        .send_window(CONN_WINDOW as u64)
        .enable_segmentation_offload(false);
    // Send the whole window in one flight, so delivery order is entirely ours.
    let mut cc = NewRenoConfig::default();
    cc.initial_window(CONN_WINDOW as u64);
    t.congestion_controller_factory(Arc::new(cc));
    t
}

fn run(delivery: Delivery, reader: Reader) -> Outcome {
    let _guard = super::subscribe();
    let mut server_cfg = server_config();
    server_cfg.transport = Arc::new(transport());
    let mut pair = Pair::new(Default::default(), server_cfg);
    let mut client_cfg = client_config();
    client_cfg.transport_config(Arc::new(transport()));
    let (cch, sch) = pair.connect_with(client_cfg);

    // Write the whole stream, then capture every datagram the client sends.
    let s = pair.client_streams(cch).open(Dir::Uni).unwrap();
    let data = vec![0xa5u8; STREAM_WINDOW as usize];
    assert_eq!(pair.client_send(cch, s).write(&data), Ok(data.len()));
    let far = Duration::from_secs(3600);
    let mut captured: Vec<Inbound> = Vec::new();
    let mut bytes = 0;
    while bytes < data.len() {
        pair.drive_client();
        while let Some((_, inbound)) = pair.server.inbound.pop_due(pair.time + far) {
            bytes += inbound.packet.len();
            captured.push(inbound);
        }
        assert!(
            pair.advance_time(),
            "client stopped before sending the stream"
        );
    }
    let sent = captured.len();
    let order: Vec<usize> = match delivery {
        Delivery::InOrder => (0..sent).collect(),
        Delivery::DropOne { index } => (0..sent).filter(|&i| i != index).collect(),
        Delivery::OddLostThenInOrder { holes } => (0..sent)
            .filter(|&i| i >= 2 * holes || i % 2 == 0)
            .collect(),
    };
    let mut slots: Vec<Option<Inbound>> = captured.into_iter().map(Some).collect();

    let mut accepted = false;
    let mut read = 0u64;
    let mut lost = None;
    let batches = order.chunks(16).count();
    for (n, batch) in order.chunks(16).enumerate() {
        for &i in batch {
            let inbound = slots[i].take().unwrap();
            pair.server.inbound.push(pair.time, inbound);
        }
        pair.drive_server();
        // The client has already sent everything; drop the server's ACKs.
        while pair.client.inbound.pop_due(pair.time + far).is_some() {}
        while let Some(event) = pair.server_conn_mut(sch).poll() {
            match event {
                Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) => {
                    assert_eq!(pair.server_streams(sch).accept(Dir::Uni), Some(s));
                    accepted = true;
                }
                Event::ConnectionLost { reason } => lost = Some(reason),
                _ => {}
            }
        }
        if lost.is_some() {
            break;
        }
        let last = n + 1 == batches;
        if accepted && (matches!(reader, Reader::Eager) || last) {
            let mut recv = pair.server_recv(sch, s);
            let mut chunks = recv.read(true).unwrap();
            loop {
                match chunks.next(usize::MAX) {
                    Ok(Some(chunk)) => {
                        assert!(chunk.bytes.iter().all(|b| *b == 0xa5));
                        read += chunk.bytes.len() as u64;
                    }
                    Ok(None) | Err(ReadError::Blocked) => break,
                    Err(e) => panic!("{e}"),
                }
            }
            let _ = chunks.finalize();
        }
    }
    Outcome { lost, read }
}

fn assert_too_many_gaps(lost: Option<ConnectionError>) {
    match lost {
        Some(ConnectionError::TransportError(e)) => {
            assert_eq!(e.code, TransportErrorCode::INTERNAL_ERROR);
            assert_eq!(e.reason, "too many gaps in stream buffer");
        }
        other => panic!("expected the gap bound to close the connection, got {other:?}"),
    }
}

#[test]
fn eager_reader_reads_full_window() {
    let outcome = run(Delivery::InOrder, Reader::Eager);
    assert_eq!(outcome.lost, None);
    assert_eq!(outcome.read, u64::from(STREAM_WINDOW));
}

/// More than `2 * MAX_CHUNKS` in-order full-size frames buffered unread are one
/// contiguous run, not one gap per frame.
#[test]
fn stalled_reader_buffers_full_window_without_abort() {
    let outcome = run(Delivery::InOrder, Reader::Stalled);
    assert_eq!(outcome.lost, None);
    assert_eq!(outcome.read, u64::from(STREAM_WINDOW));
}

/// One unrepaired hole with the rest of the window behind it is two runs.
#[test]
fn one_hole_behind_full_window_without_abort() {
    let outcome = run(Delivery::DropOne { index: 4 }, Reader::Eager);
    assert_eq!(outcome.lost, None);
    assert!(outcome.read > 0 && outcome.read < u64::from(STREAM_WINDOW));
}

/// The bound on real gaps stays: 1,100 holes still close the connection once the
/// buffer has to compact.
#[test]
fn more_than_max_chunks_real_holes_abort() {
    let outcome = run(Delivery::OddLostThenInOrder { holes: 1100 }, Reader::Eager);
    assert_too_many_gaps(outcome.lost);
}
