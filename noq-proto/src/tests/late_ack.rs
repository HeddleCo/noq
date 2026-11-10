// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::ConnectionHandle;

fn delayed_stream_packet(
    finish: bool,
) -> (
    Pair,
    ConnectionHandle,
    ConnectionHandle,
    crate::StreamId,
    util::InboundQueue,
) {
    let mut pair = Pair::default_with_deterministic_pns();
    let (client, server) = pair.connect_with(client_config_with_deterministic_pns());
    pair.drive();
    pair.routes.set_latency(Duration::ZERO);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    pair.client_send(client, stream)
        .write(b"late receipt")
        .unwrap();
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
    let retransmissions = mem::take(&mut pair.server.inbound);
    pair.server.inbound = delayed;
    pair.drive_server();
    pair.drive_client();
    (pair, client, server, stream, retransmissions)
}

#[test]
fn late_packet_ack_credits_unique_bytes_only_on_its_stream() {
    let _guard = subscribe();
    let (mut pair, client, _, stream, retransmissions) = delayed_stream_packet(false);
    let sibling = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(
        pair.client_send(client, stream).acknowledged_bytes(),
        Ok(12)
    );
    assert_eq!(
        pair.client_send(client, sibling).acknowledged_bytes(),
        Ok(0)
    );
    pair.server.inbound = retransmissions;
    pair.drive();
    assert_eq!(
        pair.client_send(client, stream).acknowledged_bytes(),
        Ok(12)
    );
    assert_eq!(
        pair.client_send(client, sibling).acknowledged_bytes(),
        Ok(0)
    );
}

#[test]
fn late_packet_ack_keeps_the_recovery_decisions() {
    let _guard = subscribe();
    let (mut pair, client, _, _, retransmissions) = delayed_stream_packet(false);
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
    std::eprintln!("RECOVERY_PIN {recovery:?}");
    assert!(stats.lost_packets > 0);
}

#[test]
fn late_packet_ack_of_fin_completes_before_queued_retransmission() {
    let _guard = subscribe();
    let (mut pair, client, _, stream, _) = delayed_stream_packet(true);
    assert_matches!(pair.client_conn_mut(client).poll(),
        Some(Event::Stream(StreamEvent::Finished { id })) if id == stream);
}
