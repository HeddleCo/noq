//! C-1509/C-1512 bounds, exercised with real encrypted packets and the simulated clock.

use alloc::{boxed::Box, sync::Arc, vec, vec::Vec};
use core::{
    mem,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use testresult::TestResult;

use crate::connection::timer::{ConnTimer, Timer};
use crate::crypto::{CryptoError, PacketKey};
use crate::{
    ConnectionError, Event, FourTuple, Instant, PathId, PathStatus, Side::*, TransportConfig,
    TransportErrorCode,
};

use super::util::{ConnPair, Inbound, server_config};

const DISCARD: Timer = Timer::Conn(ConnTimer::KeyDiscard);

/// Delegate real AEAD operations, observing key destruction or reducing the integrity limit.
struct ObservedKey {
    inner: Option<Box<dyn PacketKey>>,
    dropped: Arc<AtomicUsize>,
    integrity_limit: u64,
    confidentiality_limit: Option<u64>,
}

impl Drop for ObservedKey {
    fn drop(&mut self) {
        if self.inner.is_some() {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl PacketKey for ObservedKey {
    fn encrypt(&self, path: PathId, pn: u64, buf: &mut [u8], header_len: usize) {
        self.inner
            .as_ref()
            .unwrap()
            .encrypt(path, pn, buf, header_len);
    }
    fn decrypt(
        &self,
        path: PathId,
        pn: u64,
        header: &[u8],
        payload: &mut BytesMut,
    ) -> Result<(), CryptoError> {
        self.inner
            .as_ref()
            .unwrap()
            .decrypt(path, pn, header, payload)
    }
    fn tag_len(&self) -> usize {
        self.inner.as_ref().unwrap().tag_len()
    }
    fn confidentiality_limit(&self) -> u64 {
        self.confidentiality_limit
            .unwrap_or_else(|| self.inner.as_ref().unwrap().confidentiality_limit())
    }
    fn integrity_limit(&self) -> u64 {
        self.integrity_limit
    }
}

fn observe(
    key: &mut Box<dyn PacketKey>,
    limit: u64,
    confidentiality_limit: Option<u64>,
) -> Arc<AtomicUsize> {
    let dropped = Arc::new(AtomicUsize::new(0));
    let inner = mem::replace(
        key,
        Box::new(ObservedKey {
            inner: None,
            dropped: dropped.clone(),
            integrity_limit: limit,
            confidentiality_limit,
        }),
    );
    *key = Box::new(ObservedKey {
        inner: Some(inner),
        dropped: dropped.clone(),
        integrity_limit: limit,
        confidentiality_limit,
    });
    dropped
}

struct Flight {
    pair: ConnPair,
    fast: PathId,
    old: Vec<Inbound>,
    current: Inbound,
    first: Instant,
    dropped: Arc<AtomicUsize>,
}

impl Flight {
    fn new(idle: Option<Duration>) -> TestResult<Self> {
        let mut cc = crate::congestion::CubicConfig::default();
        cc.initial_window(4 * 1024 * 1024);
        let mut cfg = TransportConfig::default();
        cfg.max_idle_timeout(idle.map(|d| d.try_into().unwrap()));
        cfg.initial_rtt(Duration::from_secs(1));
        cfg.mtu_discovery_config(None);
        cfg.congestion_controller_factory(Arc::new(cc));
        cfg.initial_key_phase_size(Some(u64::MAX));
        cfg.max_concurrent_multipath_paths(2);
        // Only the client proposes an idle timeout. The server must use the negotiated
        // value even though its local timeout is disabled.
        let mut server_transport = cfg.clone();
        server_transport.max_idle_timeout(None);
        let mut server_cfg = server_config();
        server_cfg.transport = Arc::new(server_transport);
        let mut pair = ConnPair::builder()
            .with_transport_cfg(cfg)
            .with_server_cfg(server_cfg)
            .with_latency(Duration::from_millis(500))
            .disable_mtud_discovery()
            .connect();
        // Confirm the update on a second path while path 0's genuine old flight stays silent.
        let fast = pair.open_path(
            Client,
            FourTuple::from_remote(pair.routes.public_server_addr()),
            PathStatus::Backup,
        )?;
        pair.drive();
        let key = &mut pair.conn_mut(Server).packet_keys_for_test().remote;
        let dropped = observe(key, u64::MAX, None);
        let mut old = Vec::new();
        // These are distinct authenticated packets, held by the network rather than replayed.
        for i in 0..48 {
            pair.time += Duration::from_millis(1);
            pair.datagrams(Client)
                .send(Bytes::from(vec![i; 64]), false)?;
            pair.drive_client();
            old.extend(pair.server.inbound.drain().map(|(_, packet)| packet));
        }
        assert!(old.len() >= 40, "enough old packets for the 30 s cap");
        pair.force_key_update(Client);
        pair.conn_mut(Client).ping_path(fast)?;
        pair.drive_client();
        pair.time += Duration::from_millis(500);
        pair.drive_server();
        let first = pair.time;
        assert!(pair.conn(Server).timer_pending(DISCARD).is_some());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        // Keep a fresh packet to prove the connection/current keys remain usable after discard.
        pair.datagrams(Client)
            .send(Bytes::from_static(b"current"), false)?;
        pair.drive_client();
        let current = pair.server.inbound.pop_last().unwrap().1;
        pair.server.inbound.clear();
        Ok(Self {
            pair,
            fast,
            old,
            current,
            first,
            dropped,
        })
    }

    fn deliver(&mut self, at: Instant, packet: Inbound) {
        self.pair.time = at;
        self.pair.server.inbound.push(at, packet);
        self.pair.drive_server();
    }
}

#[test]
fn key_retention_absolute_cap() -> TestResult {
    // Cover disabled idle timeout, a timeout larger than 30 s, and the shorter idle bound.
    for idle in [
        None,
        Some(Duration::from_secs(60)),
        Some(Duration::from_secs(2)),
    ] {
        let cap = idle
            .unwrap_or(Duration::from_secs(30))
            .min(Duration::from_secs(30));
        let mut flight = Flight::new(idle)?;
        for second in 1..=cap.as_secs() {
            let packet = flight.old.remove(0);
            flight.deliver(flight.first + Duration::from_secs(second), packet);
            let received = flight.pair.datagrams(Server).recv();
            if second < cap.as_secs() {
                assert!(
                    received.is_some(),
                    "genuine old packet before cap, idle={idle:?} t={second}"
                );
                assert_eq!(flight.pair.stats(Server).authentication_drops, 0);
            } else {
                assert!(
                    received.is_none(),
                    "old keys survived absolute cap, idle={idle:?}"
                );
                assert_eq!(flight.pair.stats(Server).authentication_drops, 1);
            }
        }
        assert_eq!(flight.dropped.load(Ordering::SeqCst), 1);
        flight.deliver(flight.pair.time, flight.current.clone());
        assert_eq!(
            flight.pair.datagrams(Server).recv().unwrap(),
            b"current"[..]
        );
    }
    Ok(())
}

#[test]
fn key_retention_further_phase_change_discards_previous() -> TestResult {
    let mut flight = Flight::new(None)?;
    // Our existing update guard forbids another local phase change until discard.
    flight.pair.force_key_update(Server);
    assert_eq!(flight.dropped.load(Ordering::SeqCst), 0);
    // Resolve both client paths, then expire only its retention. The server's path 0
    // is still pending, so its old keys survive the client's independent deadline.
    flight.pair.conn_mut(Server).ping_path(PathId::ZERO)?;
    flight.pair.conn_mut(Server).ping_path(flight.fast)?;
    flight.pair.time += Duration::from_millis(500);
    flight.pair.drive_server();
    flight.pair.time += Duration::from_millis(500);
    flight.pair.drive_client();
    let client_deadline = flight.pair.conn(Client).timer_pending(DISCARD).unwrap();
    let last_old = client_deadline - Duration::from_millis(500);
    while flight.pair.time + Duration::from_secs(1) < last_old {
        let packet = flight.old.remove(0);
        flight.deliver(flight.pair.time + Duration::from_secs(1), packet);
        assert!(flight.pair.datagrams(Server).recv().is_some());
    }
    let packet = flight.old.remove(0);
    flight.deliver(last_old, packet);
    assert!(flight.pair.datagrams(Server).recv().is_some());
    // Confirm phase one on the slow path before phase two can overtake it there.
    flight.deliver(last_old, flight.current.clone());
    assert_eq!(
        flight.pair.datagrams(Server).recv().unwrap(),
        b"current"[..]
    );
    flight.pair.time = client_deadline;
    flight.pair.conn_mut(Client).handle_timeout(client_deadline);
    flight.pair.force_key_update(Client);
    flight.pair.conn_mut(Client).ping_path(flight.fast)?;
    flight.pair.drive_client();
    flight.pair.time += Duration::from_millis(500);
    flight.pair.drive_server();
    assert_eq!(
        flight.dropped.load(Ordering::SeqCst),
        1,
        "retained generation survived peer's next update"
    );
    assert_eq!(flight.pair.stats(Server).authentication_drops, 0);
    assert!(
        !flight.pair.conn(Server).is_closed(),
        "valid further key update closed the connection"
    );
    // Ours can rotate only after the replacement generation has also been discarded.
    let deadline = flight.pair.conn(Server).timer_pending(DISCARD).unwrap();
    flight.pair.conn_mut(Server).handle_timeout(deadline);
    flight.pair.force_key_update(Server);

    Ok(())
}

#[test]
fn key_retention_integrity_limit_counts_failed_previous_key_decryptions() -> TestResult {
    let mut flight = Flight::new(None)?;
    let key = &mut flight.pair.conn_mut(Server).packet_keys_for_test().local;
    observe(key, 2, None);
    let mut corrupt = flight.old.remove(0);
    // Damage the tag, outside the header-protection sample; the old-key selector is unchanged.
    let last = corrupt.packet.len() - 1;
    corrupt.packet[last] ^= 1;
    for _ in 0..3 {
        flight.deliver(flight.first, corrupt.clone());
    }
    assert_eq!(flight.pair.stats(Server).authentication_drops, 3);
    let mut closed = false;
    while let Some(event) = flight.pair.poll(Server) {
        if let Event::ConnectionLost {
            reason: ConnectionError::TransportError(error),
        } = event
        {
            assert_eq!(error.code, TransportErrorCode::AEAD_LIMIT_REACHED);
            closed = true;
        }
    }
    assert!(
        closed,
        "retained-key failures bypassed the AEAD integrity limit"
    );
    Ok(())
}

#[test]
fn key_retention_existing_discard_path_removes_read_key() -> TestResult {
    let mut flight = Flight::new(None)?;
    let packet = flight.old.remove(0);
    flight.deliver(flight.first + Duration::from_secs(1), packet);
    assert!(flight.pair.datagrams(Server).recv().is_some());
    assert_eq!(flight.pair.stats(Server).authentication_drops, 0);
    // Fire exactly the existing KeyDiscard timer after the last genuine old packet.
    let deadline = flight.pair.conn(Server).timer_pending(DISCARD).unwrap();
    let packet = flight.old.remove(0);
    flight.deliver(deadline, packet);
    assert!(
        flight.pair.datagrams(Server).recv().is_none(),
        "a read-key copy outlived KeyDiscard"
    );
    assert_eq!(flight.pair.stats(Server).authentication_drops, 1);
    assert_eq!(flight.dropped.load(Ordering::SeqCst), 1);
    flight.deliver(deadline, flight.current.clone());
    assert_eq!(
        flight.pair.datagrams(Server).recv().unwrap(),
        b"current"[..]
    );
    Ok(())
}

#[test]
fn key_retention_replay_does_not_extend_discard() -> TestResult {
    let mut flight = Flight::new(None)?;
    let duplicates = flight.pair.stats(Server).duplicate_discards;
    let packet = flight.old.remove(0);
    flight.deliver(flight.first + Duration::from_secs(1), packet.clone());
    assert!(flight.pair.datagrams(Server).recv().is_some());
    let deadline = flight.pair.conn(Server).timer_pending(DISCARD).unwrap();
    flight.deliver(flight.first + Duration::from_millis(1500), packet);
    assert!(flight.pair.datagrams(Server).recv().is_none());
    assert_eq!(flight.pair.stats(Server).duplicate_discards, duplicates + 1);
    let old = flight.old.remove(0);
    flight.deliver(deadline, old);
    assert!(
        flight.pair.datagrams(Server).recv().is_none(),
        "replay extended retention"
    );
    assert_eq!(flight.pair.stats(Server).authentication_drops, 1);
    Ok(())
}

#[test]
fn key_retention_close_discards_previous_keys() -> TestResult {
    let mut flight = Flight::new(None)?;
    // Closing cancels timers, so it must not leave a read key alive without its cap.
    let now = flight.pair.time;
    flight
        .pair
        .conn_mut(Server)
        .close(now, 0u32.into(), Bytes::new());
    assert_eq!(
        flight.dropped.load(Ordering::SeqCst),
        1,
        "closing cancelled the cap but retained its key"
    );
    Ok(())
}

/// A pending slow path must not postpone the sender's scheduled phase change.
#[test]
fn key_retention_scheduled_update_releases_pending_paths() -> TestResult {
    let mut flight = Flight::new(None)?;
    // Confirm this phase on both client paths while the server's slow path remains pending.
    flight.pair.conn_mut(Server).ping_path(PathId::ZERO)?;
    flight.pair.conn_mut(Server).ping_path(flight.fast)?;
    flight.pair.drive_server();
    flight.pair.time += Duration::from_millis(500);
    flight.pair.drive_client();
    // This phase has sent packets already; a one-packet budget makes the next update due.
    let key = &mut flight.pair.conn_mut(Server).packet_keys_for_test().local;
    observe(key, u64::MAX, Some(1));
    let deadline = flight.pair.conn(Server).timer_pending(DISCARD).unwrap();
    assert!(deadline > flight.pair.time);
    assert_eq!(flight.dropped.load(Ordering::SeqCst), 0);

    flight
        .pair
        .datagrams(Server)
        .send(Bytes::from_static(b"scheduled update"), false)?;
    flight.pair.drive_server();
    assert_eq!(
        flight.dropped.load(Ordering::SeqCst),
        1,
        "scheduled update was blocked by retained previous keys"
    );
    assert!(flight.pair.time < deadline, "update waited for key discard");
    flight.pair.time += Duration::from_millis(500);
    flight.pair.drive_client();
    assert_eq!(
        flight.pair.datagrams(Client).recv().unwrap(),
        b"scheduled update"[..]
    );
    assert_eq!(flight.pair.stats(Client).authentication_drops, 0);
    assert!(!flight.pair.conn(Server).is_closed());
    Ok(())
}
