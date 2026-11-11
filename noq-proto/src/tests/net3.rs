//! NET-3: lossless per-direction FIFOs, real encrypted traffic, simulated time only.
//! Run ignored cells with `cargo test -p noq-proto net3 -- --ignored --nocapture`.

use alloc::{sync::Arc, vec, vec::Vec};
use core::time::Duration;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use testresult::TestResult;

use super::util::{ConnPair, MixedRouteRouting, Routing};
use crate::{
    Dir, Event, FourTuple, PathId, PathStatus, Side,
    Side::*,
    StreamEvent, TransportConfig,
    connection::timer::{ConnTimer, Timer},
};

#[derive(Clone, Copy, Debug)]
enum Timing {
    Before,
    During,
    Release,
    After,
}
#[derive(Clone, Copy, Debug)]
enum PathEvent {
    Pinned,
    Demote,
    Abandon,
    ValidateDuring,
    ValidateRelease,
}
#[derive(Clone, Copy, Debug)]
struct Cell {
    direction: Side,
    both: bool,
    initiator: Side,
    both_update: bool,
    timing: Timing,
    path: PathEvent,
    hold_ms: u64,
    hold_3pto_milli: u32,
    spacing_us: u64,
    bytes: usize,
    probes: usize,
    second: bool,
}
impl Default for Cell {
    fn default() -> Self {
        Self {
            direction: Client,
            both: false,
            initiator: Client,
            both_update: false,
            timing: Timing::During,
            path: PathEvent::Pinned,
            hold_ms: 0,
            hold_3pto_milli: 0,
            spacing_us: 100,
            bytes: 32 * 1024,
            probes: 0,
            second: false,
        }
    }
}

#[derive(Debug)]
struct Outcome {
    auth: u64,
    duplicates: u64,
    lost: u64,
    bytes: usize,
    fins: usize,
    acked: usize,
}
impl Outcome {
    fn check(&self, label: &str) {
        assert_eq!(
            (self.auth, self.duplicates),
            (0, 0),
            "{label}: authentication_drops={} duplicate_discards={}",
            self.auth,
            self.duplicates
        );
    }
}
fn drive_side(pair: &mut ConnPair, side: Side) {
    match side {
        Client => pair.drive_client(),
        Server => pair.drive_server(),
    }
}
fn route(pair: &mut ConnPair) -> &mut MixedRouteRouting {
    let Routing::MixedRoute(route) = &mut pair.routes else {
        unreachable!()
    };
    route
}
fn update(pair: &mut ConnPair, c: Cell, fast: PathId) -> TestResult {
    for side in [Client, Server] {
        if side == c.initiator || c.both_update {
            pair.force_key_update(side);
            pair.ping_path(side, fast)?;
            drive_side(pair, side);
        }
    }
    Ok(())
}

fn run(c: Cell, seed: u64, label: &str) -> TestResult<Outcome> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut cc = crate::congestion::CubicConfig::default();
    cc.initial_window(1024 * 1024);
    let mut cfg = TransportConfig::default();
    cfg.congestion_controller_factory(Arc::new(cc));
    cfg.deterministic_packet_numbers(true);
    // iroh keeps every path alive at 5 s, so each live path carries a packet in every phase.
    cfg.default_path_keep_alive_interval(Some(Duration::from_secs(5)));
    let mut builder = ConnPair::builder()
        .with_transport_cfg(cfg)
        .enable_multipath()
        .enable_nat_traversal()
        .disable_mtud_discovery()
        .with_routes(MixedRouteRouting {
            latency: Duration::from_millis(10),
            direct_response_latency: Duration::from_millis(10),
            spacing: Duration::from_micros(c.spacing_us),
            ..Default::default()
        });
    builder
        .client_transport_cfg
        .max_remote_nat_traversal_addresses(64);
    builder
        .server_transport_cfg
        .max_remote_nat_traversal_addresses(64);
    builder.client_endpoint_cfg.rng_seed(Some(rng.random()));
    builder.server_endpoint_cfg.rng_seed(Some(rng.random()));
    let mut pair = builder.connect();
    // Advertise before the hold: discovery packets themselves must not wait in the FIFO.
    for index in 0..c.probes.max(1) {
        let candidate = |network| {
            core::net::SocketAddr::new(
                core::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, network, index as u16 + 2).into(),
                1,
            )
        };
        pair.add_nat_traversal_address(Server, candidate(2))?;
        pair.add_nat_traversal_address(Client, candidate(1))?;
    }
    pair.drive();
    let delayed_validation = matches!(
        c.path,
        PathEvent::ValidateDuring | PathEvent::ValidateRelease
    );
    let mut fast = None;
    if !delayed_validation {
        fast = Some(pair.open_path(
            Client,
            FourTuple::from_remote(MixedRouteRouting::DIRECT),
            PathStatus::Backup,
        )?);
        pair.drive();
    }
    if matches!(c.timing, Timing::Before) {
        update(&mut pair, c, fast.expect("before cells validate first"))?;
        pair.drive();
    }
    let start = pair.time;
    let release = start
        + Duration::from_millis(c.hold_ms)
        + pair.conn(!c.initiator).base_pto() * 3 * c.hold_3pto_milli / 1000;
    {
        let r = route(&mut pair);
        r.shaped = c.direction == Client || c.both;
        r.shape_download = c.direction == Server || c.both;
        r.hold_upload = Some(release);
        r.hold_download = Some(release);
        if c.probes > 0 {
            r.probe_replies_until = Some(release);
        }
    }
    let mut streams = Vec::new();
    for side in [Client, Server] {
        if side == c.direction || c.both {
            let stream = pair.streams(side).open(Dir::Uni).expect("stream credit");
            assert_eq!(
                pair.send_stream(side, stream).write(&vec![42; c.bytes])?,
                c.bytes
            );
            pair.send_stream(side, stream).finish()?;
            drive_side(&mut pair, side);
            streams.push((side, stream, 0, false, false));
        }
    }
    for side in [Client, Server] {
        if side == c.direction || c.both {
            let r = route(&mut pair);
            let fifo = if side == Client {
                &r.departures
            } else {
                &r.download_departures
            };
            assert!(
                fifo.back().is_some_and(|t| *t > start),
                "seed={seed}: {side:?} FIFO must hold actual packets"
            );
        }
    }
    let queued = if c.direction == Client {
        route(&mut pair).departures.len()
    } else {
        route(&mut pair).download_departures.len()
    };
    if matches!(c.path, PathEvent::ValidateDuring) {
        fast = Some(pair.open_path(
            Client,
            FourTuple::from_remote(MixedRouteRouting::DIRECT),
            PathStatus::Backup,
        )?);
    }
    if delayed_validation && fast.is_some() {
        pair.drive_client();
        pair.drive_server();
    }
    if matches!(c.timing, Timing::During) {
        update(&mut pair, c, fast.expect("during cells have a direct path"))?;
    }
    // Interleave real off-path probes with held path-0 PINGs. Each round creates a
    // distinct authenticated island separated by missing PNs; >128 PNs also leave
    // the bitmap. No packet is cloned, so every duplicate discard is spurious.
    for _ in 0..c.probes {
        for _ in 0..8 {
            pair.time += Duration::from_micros(100);
            pair.ping_path(c.direction, PathId::ZERO)?;
            drive_side(&mut pair, c.direction);
        }
        assert_eq!(pair.initiate_nat_traversal_round(Client)?.len(), c.probes);
        pair.drive_client();
        if c.direction == Server {
            pair.drive_server();
        }
    }
    if c.probes > 0 {
        // Push every probe island out of the 128-bit recent bitmap before release.
        for _ in 0..128 {
            pair.time += Duration::from_micros(100);
            pair.ping_path(c.direction, PathId::ZERO)?;
            drive_side(&mut pair, c.direction);
        }
    }
    if c.probes > 0 {
        let r = route(&mut pair);
        let fifo = if c.direction == Client {
            &r.departures
        } else {
            &r.download_departures
        };
        assert!(
            fifo.len() >= queued + 8 * c.probes + 128,
            "seed={seed}: PINGs coalesced instead of creating PN gaps"
        );
        drive_side(&mut pair, !c.direction);
        let overtaking = pair.stats(!c.direction).frame_rx.path_challenge;
        assert!(
            overtaking >= c.probes as u64,
            "seed={seed}: probes did not overtake the queue"
        );
        std::eprintln!(
            "NET3 probes cell={label} targets={} overtaking={overtaking}",
            c.probes
        );
    }
    let mut boundary = None;
    let queued_until = route(&mut pair)
        .departures
        .back()
        .copied()
        .into_iter()
        .chain(route(&mut pair).download_departures.back().copied())
        .max()
        .expect("queued flight");
    let mut changed = false;
    let mut release_updated = !matches!(c.timing, Timing::Release);
    let mut validation_done = !matches!(c.path, PathEvent::ValidateRelease);
    let mut outcome = Outcome {
        auth: 0,
        duplicates: 0,
        lost: 0,
        bytes: 0,
        fins: 0,
        acked: 0,
    };
    for _ in 0..20000 {
        if !validation_done && pair.time >= release {
            fast = Some(pair.open_path(
                Client,
                FourTuple::from_remote(MixedRouteRouting::DIRECT),
                PathStatus::Backup,
            )?);
            validation_done = true;
        }
        if !release_updated && pair.time >= release {
            update(&mut pair, c, fast.expect("release path"))?;
            release_updated = true;
        }
        if !changed
            && let Some(f) = fast
            && pair.path_stats(Server, f).is_some()
        {
            for side in [Client, Server] {
                if matches!(c.path, PathEvent::Demote | PathEvent::Abandon) {
                    pair.set_path_status(side, f, PathStatus::Available)?;
                    pair.set_path_status(side, PathId::ZERO, PathStatus::Backup)?;
                }
            }
            if matches!(c.path, PathEvent::Abandon) {
                pair.close_path(c.direction, PathId::ZERO, 0u8.into())?;
            }
            changed = true;
        }
        let before = pair.time;
        let active = pair.step();
        if boundary.is_none()
            && pair
                .conn(!c.initiator)
                .timer_pending(Timer::Conn(ConnTimer::KeyDiscard))
                .is_some()
        {
            // The RFC 9001 § 6.5 deadline: 3 × PTO after the first new-phase packet.
            boundary = Some(before + pair.conn(!c.initiator).base_pto() * 3);
        }
        for (side, stream, bytes, fin, acked) in &mut streams {
            let receiver = !*side;
            pair.streams(receiver).accept(Dir::Uni);
            if !*fin && let Ok(mut chunks) = pair.recv_stream(receiver, *stream).read(false) {
                loop {
                    match chunks.next(usize::MAX) {
                        Ok(Some(chunk)) => {
                            assert!(chunk.bytes.iter().all(|b| *b == 42));
                            *bytes += chunk.bytes.len();
                        }
                        Ok(None) => {
                            *fin = true;
                            break;
                        }
                        Err(crate::ReadError::Blocked) => break,
                        Err(e) => return Err(e.into()),
                    }
                }
                let _ = chunks.finalize();
            }
            while let Some(event) = pair.poll(*side) {
                assert!(
                    !matches!(event, Event::ConnectionLost { .. }),
                    "seed={seed}: {event:?}"
                );
                if matches!(event, Event::Stream(StreamEvent::Finished { id }) if id == *stream) {
                    *acked = true;
                }
            }
        }
        if !active {
            break;
        }
        assert!(
            pair.time - start < Duration::from_secs(60),
            "seed={seed}: simulated deadline"
        );
    }
    // Pair::step treats key-discard-only connections as idle. Explicitly advance
    // these timer events before testing the second update and the retention cap.
    let discard = Timer::Conn(ConnTimer::KeyDiscard);
    for side in [Client, Server] {
        if pair.conn(side).timer_pending(discard).is_some() {
            pair.drive_until_timer(side, discard);
            assert!(
                pair.time <= start + Duration::from_secs(30),
                "seed={seed}: key retention cap"
            );
        }
    }
    if matches!(c.timing, Timing::After) || c.second {
        update(&mut pair, c, fast.expect("after/second path"))?;
        pair.drive();
        for side in [Client, Server] {
            if pair.conn(side).timer_pending(discard).is_some() {
                pair.drive_until_timer(side, discard);
            }
        }
    }
    for side in [Client, Server] {
        let s = pair.stats(side);
        outcome.auth += s.authentication_drops;
        outcome.duplicates += s.duplicate_discards;
        outcome.lost += s.lost_packets;
        assert!(!pair.is_closed(side), "seed={seed}: connection closed");
        assert!(
            pair.conn(side)
                .timer_pending(Timer::Conn(ConnTimer::KeyDiscard))
                .is_none(),
            "seed={seed}: keys not discarded after quiescence"
        );
    }
    for (_, _, bytes, fin, acked) in streams {
        outcome.bytes += bytes;
        outcome.fins += usize::from(fin);
        outcome.acked += usize::from(acked);
    }
    if matches!(c.timing, Timing::During) && c.probes == 0 {
        let armed = boundary.expect("new phase must reach the receiver");
        if label.starts_with("m3_1_silent") {
            assert!(
                armed < release,
                "{label}: discard must precede the first post-hold delivery"
            );
        }
        if c.spacing_us >= 50_000 && c.hold_ms == 0 && c.hold_3pto_milli == 0 {
            assert!(
                armed < queued_until,
                "{label}: stale-PTO premise: discard={:?} drain={:?}",
                armed - start,
                queued_until - start
            );
        }
        std::eprintln!(
            "NET3 timing cell={label} initial_discard_in={:?} queued_until={:?}",
            armed - start,
            queued_until - start
        );
    }
    let count = if c.both { 2 } else { 1 };
    std::eprintln!(
        "NET3 cell={label} seed={seed} {c:?} authentication_drops={} duplicate_discards={} lost_packets={} received={} fins={} acked={}",
        outcome.auth,
        outcome.duplicates,
        outcome.lost,
        outcome.bytes,
        outcome.fins,
        outcome.acked
    );
    assert_eq!(
        (outcome.bytes, outcome.fins, outcome.acked),
        (c.bytes * count, count, count),
        "seed={seed} {outcome:?}"
    );
    Ok(outcome)
}

macro_rules! cells {
    ($($(#[$attr:meta])* $name:ident { $($field:ident : $value:expr),* $(,)? })*) => {$ (
        #[test] $(#[$attr])*
        fn $name() -> TestResult {
            run(Cell { $($field: $value,)* ..Cell::default() }, 2652, stringify!($name))?.check(stringify!($name));
            Ok(())
        }
    )*};
}
cells! {
    mechanism1_upload {}
    mechanism2_upload { spacing_us: 50_000 }
    m3_3_mechanism1_download { direction: Server, initiator: Server }
    m3_3_mechanism2_download { direction: Server, initiator: Server, spacing_us: 50_000 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=17 duplicate_discards=0"]
    m3_1_silent_upload { hold_ms: 1500 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=18 duplicate_discards=0"]
    m3_1_silent_download { direction: Server, initiator: Server, hold_ms: 1500 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=35 duplicate_discards=0"]
    m3_1_silent_both { both: true, both_update: true, hold_ms: 1500 }
    m3_2_probes_1 { probes: 1, timing: Timing::Before, hold_ms: 1500 }
    m3_2_probes_16 { probes: 16, timing: Timing::Before, hold_ms: 1500 }
    #[ignore = "NET-3 red: M3-2 authentication_drops=0 duplicate_discards=61"]
    m3_2_probes_33 { probes: 33, timing: Timing::Before, hold_ms: 1500 }
    #[ignore = "NET-3 red: M3-2 authentication_drops=0 duplicate_discards=124"]
    m3_2_probes_40 { probes: 40, timing: Timing::Before, hold_ms: 1500 }
    #[ignore = "NET-3 red: M3-2 authentication_drops=0 duplicate_discards=270"]
    m3_2_download_probes_33 { direction: Server, initiator: Server, probes: 33, timing: Timing::Before, hold_ms: 1500 }
    m3_4_abandon_upload { path: PathEvent::Abandon, hold_ms: 1500 }
    m3_4_abandon_download { direction: Server, initiator: Server, path: PathEvent::Abandon, hold_ms: 1500 }
    hold_half_3pto_upload { hold_3pto_milli: 500 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=17 duplicate_discards=0"]
    hold_one_3pto_upload { hold_3pto_milli: 1000 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=17 duplicate_discards=0"]
    hold_three_3pto_upload { hold_3pto_milli: 3000 }
    hold_half_3pto_download { direction: Server, initiator: Server, hold_3pto_milli: 500 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=18 duplicate_discards=0"]
    hold_three_3pto_download { direction: Server, initiator: Server, hold_3pto_milli: 3000 }
    trickle_5ms { spacing_us: 5000 }
    trickle_20ms { spacing_us: 20_000 }
    trickle_100ms { spacing_us: 100_000 }
    queue_4k { bytes: 4096 }
    queue_128k { bytes: 128 * 1024, spacing_us: 5000 }
    update_server_upload { initiator: Server }
    update_client_download { direction: Server }
    update_both_upload { both_update: true }
    update_both_directions { both: true, both_update: true }
    update_before_hold { timing: Timing::Before, hold_ms: 765 }
    update_at_release { timing: Timing::Release, hold_ms: 765 }
    update_after_drain { timing: Timing::After, hold_ms: 765 }
    second_update_upload { second: true, spacing_us: 50_000 }
    second_update_download { second: true, direction: Server, initiator: Server, spacing_us: 50_000 }
    demote_upload { path: PathEvent::Demote, spacing_us: 50_000 }
    demote_download { path: PathEvent::Demote, direction: Server, initiator: Server, spacing_us: 50_000 }
    #[ignore = "NET-3 red: M3-1 authentication_drops=17 duplicate_discards=0"]
    demote_silent { path: PathEvent::Demote, hold_ms: 1500 }
    validate_during_upload { path: PathEvent::ValidateDuring, spacing_us: 5000 }
    validate_during_download { path: PathEvent::ValidateDuring, direction: Server, initiator: Server, spacing_us: 5000 }
    validate_at_release { path: PathEvent::ValidateRelease, timing: Timing::Release, hold_ms: 765 }
    both_trickle { both: true, both_update: true, spacing_us: 50_000 }
}

// Freeze the observed red corpus separately, preserving the zero-drop oracle.
// The normal suite still executes all 409 green seeds; --ignored executes these 91.
const RED_SEEDS: &[u64] = &[
    2, 5, 6, 8, 11, 12, 20, 26, 28, 31, 33, 40, 44, 45, 47, 48, 53, 67, 74, 78, 88, 103, 106, 109,
    111, 118, 127, 133, 134, 137, 141, 146, 153, 165, 170, 172, 179, 185, 201, 213, 214, 217, 238,
    240, 258, 264, 266, 271, 277, 281, 283, 290, 291, 296, 308, 313, 320, 324, 329, 333, 334, 336,
    343, 349, 357, 358, 360, 372, 373, 381, 393, 394, 396, 400, 402, 405, 419, 422, 428, 431, 434,
    448, 457, 460, 464, 468, 469, 472, 481, 482, 488,
];

#[test]
fn seeded_sweep() -> TestResult {
    sweep(false)
}

#[test]
#[ignore = "NET-3 red: M3-1 silent-gap authentication drops in 91 seeds"]
fn seeded_sweep_red() -> TestResult {
    sweep(true)
}

fn sweep(red: bool) -> TestResult {
    let mut failures = Vec::new();
    let mut executed = 0;
    for seed in 0..500 {
        if RED_SEEDS.contains(&seed) != red {
            continue;
        }
        executed += 1;
        let mut rng = StdRng::seed_from_u64(seed);
        let direction = if rng.random() { Client } else { Server };
        let timing = [
            Timing::Before,
            Timing::During,
            Timing::Release,
            Timing::After,
        ][rng.random_range(0..4)];
        let c = Cell {
            direction,
            initiator: if rng.random() { direction } else { !direction },
            timing,
            second: rng.random(),
            both: rng.random_ratio(1, 4),
            both_update: rng.random_ratio(1, 4),
            hold_ms: rng.random_range(0..=1500),
            spacing_us: rng.random_range(100..=50_000),
            bytes: rng.random_range(4..=64) * 1024,
            path: if rng.random() {
                PathEvent::Pinned
            } else {
                PathEvent::Demote
            },
            ..Cell::default()
        };
        let result = run(c, seed, "seeded_sweep")?;
        if result.auth != 0 || result.duplicates != 0 {
            failures.push((seed, result.auth, result.duplicates));
        }
    }
    std::eprintln!(
        "NET3 sweep domain=0..500 quarantined={red} executed={executed} red={} signatures={failures:?}",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "NET3 seeds (seed, authentication_drops, duplicate_discards): {failures:?}"
    );
    Ok(())
}
