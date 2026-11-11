// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::ConnectionHandle;

fn delayed_stream_packet(
    finish: bool,
    expire: bool,
    with_sibling: bool,
) -> (
    Pair,
    ConnectionHandle,
    ConnectionHandle,
    crate::StreamId,
    crate::StreamId,
    util::InboundQueue,
) {
    let mut pair = Pair::default_with_deterministic_pns();
    let (client, server) = pair.connect_with(client_config_with_deterministic_pns());
    pair.drive();
    pair.routes.set_latency(Duration::ZERO);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    let sibling = pair.client_streams(client).open(Dir::Uni).unwrap();
    pair.client_send(client, stream)
        .write(b"late receipt")
        .unwrap();
    if with_sibling {
        pair.client_send(client, sibling)
            .write(b"sibling!")
            .unwrap();
    }
    if finish {
        pair.client_send(client, stream).finish().unwrap();
    }
    pair.drive_client();
    let delayed = mem::take(&mut pair.server.inbound);
    assert!(!delayed.is_empty());
    for _ in 0..3 {
        pair.time += Duration::from_millis(1);
        pair.client_conn_mut(client).ping();
        pair.drive_client();
    }
    pair.drive_server();
    pair.drive_client();
    assert!(pair.client_conn_mut(client).stats().lost_packets > 0);
    assert!(pair.client_conn_mut(client).stats().frame_tx.stream >= 2);
    let retransmissions = mem::take(&mut pair.server.inbound);
    if expire {
        let retention = 3 * pair.client_conn_mut(client).base_pto();
        pair.time += retention;
    }
    pair.server.inbound = delayed;
    pair.drive_server();
    pair.drive_client();
    (pair, client, server, stream, sibling, retransmissions)
}

#[test]
fn late_packet_ack_credits_unique_bytes_only_on_its_stream() {
    let _guard = subscribe();
    let (mut pair, client, _, stream, sibling, retransmissions) =
        delayed_stream_packet(false, false, true);
    assert_eq!(
        pair.client_send(client, stream).acknowledged_bytes(),
        Ok(12)
    );
    assert_eq!(
        pair.client_send(client, sibling).acknowledged_bytes(),
        Ok(8)
    );
    pair.server.inbound = retransmissions;
    pair.drive();
    assert_eq!(
        pair.client_send(client, stream).acknowledged_bytes(),
        Ok(12)
    );
    assert_eq!(
        pair.client_send(client, sibling).acknowledged_bytes(),
        Ok(8)
    );
}

#[test]
fn late_packet_ack_keeps_the_recovery_decisions() {
    let _guard = subscribe();
    let (mut pair, client, _, _, _, retransmissions) = delayed_stream_packet(false, false, false);
    let stats = pair
        .client_conn_mut(client)
        .path_stats(PathId::ZERO)
        .unwrap();
    let recovery = (
        stats.cwnd,
        stats.lost_packets,
        stats.lost_bytes,
        stats.congestion_events,
        stats.spurious_congestion_events,
        stats.frame_tx.stream,
        retransmissions.len(),
    );
    // Recorded with the late-ACK fix reverted: recovery must stay identical.
    assert_eq!(recovery, (12000, 1, 41, 1, 1, 2, 1));
}

#[test]
fn late_packet_ack_of_fin_completes_before_queued_retransmission() {
    let _guard = subscribe();
    let (mut pair, client, _, stream, _, _) = delayed_stream_packet(true, false, false);
    assert_matches!(pair.client_conn_mut(client).poll(),
        Some(Event::Stream(StreamEvent::Finished { id })) if id == stream);
}

#[test]
fn late_packet_metadata_expires_after_three_base_ptos() {
    let _guard = subscribe();
    let (mut pair, client, _, stream, _, retransmissions) =
        delayed_stream_packet(false, true, false);
    assert_eq!(pair.client_send(client, stream).acknowledged_bytes(), Ok(0));
    pair.server.inbound = retransmissions;
    pair.drive();
    assert_eq!(
        pair.client_send(client, stream).acknowledged_bytes(),
        Ok(12)
    );
}

#[test]
fn ack_work_is_independent_of_unrelated_retained_packets() {
    let _guard = subscribe();
    for count in [8, 64] {
        let mut pair = Pair::default_with_deterministic_pns();
        let (client, _) = pair.connect_with(client_config_with_deterministic_pns());
        pair.drive();
        pair.routes.set_latency(Duration::ZERO);
        for _ in 0..count {
            let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
            pair.client_send(client, stream).write(b"retained").unwrap();
            pair.time += Duration::from_millis(1);
            pair.drive_client();
            pair.server.inbound.clear();
        }
        for _ in 0..3 {
            pair.time += Duration::from_millis(1);
            pair.client_conn_mut(client).ping();
            pair.drive_client();
        }
        pair.drive_server();
        pair.drive_client();
        pair.server.inbound.clear(); // Withhold all queued retransmissions.
        let (retained, _) = pair.client_conn_mut(client).late_stream_work();
        assert_eq!(retained, count);
        let now = pair.time;
        pair.client_conn_mut(client)
            .repeat_last_ack_for_work_test(now);
        let (remaining, visits) = pair.client_conn_mut(client).late_stream_work();
        std::eprintln!(
            "unrelated retained={retained}, remaining={remaining}, ACK frame visits={visits}"
        );
        assert_eq!(remaining, retained);
        assert!(
            visits <= 1,
            "ACK must not visit unrelated retained metadata"
        );
    }
}
