# Per-transport send backpressure

## Problem

`noq` accepts one `AsyncUdpSocket` and creates a `UdpSender` for each driver task. Downstream
implementations can multiplex UDP, relay, and custom transports behind that object. Before this
change, `UdpSender::poll_send` could report only success, failure, or `Poll::Pending`.
`Poll::Pending` means the sender as a whole is blocked and parks the connection driver. A mux could
therefore neither retain a packet for one blocked destination nor continue using another writable
destination. Dropping the blocked packet was the only way to avoid head-of-line blocking, turning
local capacity pressure into apparent network loss.

## Runtime API

`UdpSender::poll_send` keeps its signature and gains one documented semantic: returning an error
of kind `io::ErrorKind::WouldBlock`, after registering the supplied waker, means the transport
serving this transmit's destination cannot currently accept it. The driver retains the transmit,
continues sending to other destinations, and retries when that waker fires. `Poll::Pending` keeps
its sender-wide meaning and all other errors remain fatal, exactly as today.

No existing implementor changes behavior: surfacing `WouldBlock` from `poll_send` is fatal to the
connection today, so no working sender does it — `noq-udp` retries `WouldBlock` internally and
never surfaces it, and is unchanged. Senders that multiplex independently writable transports,
the only intended producers, opt in explicitly by returning it.

## Retention and scheduling

The connection driver retries retained transmits before asking proto for new ones. On
`WouldBlock`, it copies the already encoded bytes into a bounded retained set and marks the
transmit's `FourTuple` transport-blocked in `noq-proto`. Proto omits that 4-tuple from on-path, MTU,
previous-path, off-path response, and NAT-probe scheduling while continuing to inspect other paths.
Because the mark is installed before polling proto again, there can be at most one retained
transmit per blocked 4-tuple. The bound is therefore the number of independently scheduled network
paths, not the amount of queued application data.

A subsequent driver poll tries every transmit that was retained at the start of that poll once.
Returning `WouldBlock` again does not self-wake or form a ready loop. A legacy `Poll::Pending`
still stops the entire send pass, preserving its sender-wide meaning. When a retained transmit is
accepted, the driver sends the exact stored bytes, clears the proto mark, and resumes ordinary
scheduling for that 4-tuple.

For a single-path connection, proto has no eligible transmit source while the transport is
blocked. After registering the sender waker, the driver returns `Poll::Pending` and remains parked
until a sender wake, application event, packet arrival, or timer occurs. Incidental wakes may retry
the same retained transmit once, but cannot allocate another one or spin.

## Packet accounting, pacing, and timers

`Connection::poll_transmit` commits packet numbers, stream ranges, congestion accounting, pacing
tokens, and sent-packet metadata while encoding. Moving that mutation to an I/O completion
callback would be a much larger protocol refactor, so a retained transmit keeps its encode-time
accounting and is sent exactly once. This matches noq's existing behavior for sender-wide
backpressure: when a plain UDP socket returns `Pending`, the driver already buffers the encoded
transmit and retries it later without touching timestamps.

While a 4-tuple is transport-blocked, its paths' loss-detection timers are stopped: no
retransmission can make progress into a blocked transport, and letting the PTO fire would only
inflate the backoff counter. On unblock the timers are re-armed from the unmodified sent-packet
state.

Because timestamps are not modified, the consequences of a block longer than the loss/PTO
deadlines are the same as for a long sender-wide block today: the retained packet's eventual RTT
sample is inflated by the local wait (inflated samples cannot corrupt `min_rtt`, which is a
monotone minimum), and re-arming after a long block may fire an immediate loss timeout or probe
for the retained packet, producing a benign duplicate. For kernel UDP sockets such blocks last
microseconds and this never matters. Transports that block for tens of milliseconds or more may
justify rebasing the retained packets' timestamps to the transport-acceptance time; that is
deliberately left as a follow-up increment ("precise held-packet rebase") so this core stays
small.

Pacing state remains intact. Once writable, the retained datagram is sent first and later packets
are still subject to the existing congestion window and pacer. Loss, pacing, ACK, keepalive, path
validation, path-idle, and connection-idle timers on other paths continue normally. Connection and
path idle timers are deliberately not suspended, so an indefinitely blocked connection still
observes its negotiated liveness limits.

Off-path path-validation and NAT-traversal packets are encoded without congestion/loss tracking in
the existing implementation. They are still retained and suppressed by exact 4-tuple, but need no
loss-timer handling.

## Performance

Nothing runs on the unblocked hot path except membership checks against an empty
`FxHashSet<FourTuple>` when scheduling transmits and arming loss timers. All other logic runs
only while a path is blocked or at the block/unblock transitions.

## Endpoint-generated packets

Initial stateless responses, version negotiation, Retry, refusal responses, and stateless resets
are produced by the endpoint driver without connection-owned retransmission state. That path is
unchanged: it already ignored send results, so a `WouldBlock` there means the response is dropped
and the peer retries the triggering packet, exactly as before.

## Why this is a stepping stone toward transport ownership

A complete solution to noq issue #403 could give proto first-class transport objects, independent
send queues, per-transport configuration, and explicit ownership of readiness. That would also
help per-transport GSO limits and transport configuration, but it is an invasive API change for
single-socket users and for `noq-udp`.

This change carries the minimum information currently lost at the mux boundary: one destination is
blocked while others may proceed. It puts scheduling suppression in `noq-proto`, leaves
`noq-udp`'s public API untouched, and makes the runtime extension opt-in and source-compatible.
Future transport objects can replace the `FourTuple` key without changing the retain-and-retry
semantics established here.

## Alternatives considered

- Add a second trait method returning a sent/blocked outcome enum, with a default implementation
  delegating to `poll_send`. This keeps the blocked signal out of the error channel and fails
  loud if an implementor leaks an OS-level `WouldBlock` without registering a waker, but doubles
  the send entry points and adds API that a breaking transport refactor would immediately
  collapse again. Because surfacing `WouldBlock` is fatal today, reusing it cannot change any
  working sender's behavior, and the leak failure mode (a limping path rather than a dead
  connection) is the same trust the contract already extends for waker registration on
  `Pending`.
- Keep generating and queue all blocked transmits in the runtime. This is unbounded and allows
  proto to consume stream data and packet numbers faster than a transport can drain.
- Drop blocked transmits and rely on QUIC recovery. This is the existing failure mode: congestion
  control observes local queue pressure as path loss.
- Defer packet construction until transport readiness. The current sender API cannot reserve
  readiness for a destination, and reconstructing later must preserve packet numbers, ACK ranges,
  stream selection, keys, and pacing decisions. Retaining encoded bytes is smaller and exact.
- Put the richer result in `noq-udp`. The mux exists above `noq-udp`, and changing that crate would
  force a fork on ordinary UDP users without conveying transport identity to proto.

## Open questions and follow-ups

- The precise held-packet rebase described above: track the packet numbers encoded into the
  retained transmit and rebase exactly those to the transport-acceptance time on unblock, making
  their RTT samples correct and eliminating the post-block duplicate for slow transports.
- A future transport identity should likely be an opaque stable key rather than a `FourTuple`,
  especially across migration or when multiple logical transports can serve one destination.
- A first-class transport scheduler could expose independent `max_transmit_segments`, MTU,
  congestion policy, and readiness registration, subsuming sibling work tracked alongside #403.
- If muxes need more than one connection task waiting on independently writable destinations, the
  readiness contract may eventually benefit from explicit per-transport registration tokens rather
  than repeated use of a task waker.
