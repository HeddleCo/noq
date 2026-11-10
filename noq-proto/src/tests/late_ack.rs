// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::ConnectionHandle;

fn delayed_stream_packet() -> (
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
    let (mut pair, client, _, stream, retransmissions) = delayed_stream_packet();
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
