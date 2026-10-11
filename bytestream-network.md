# Byte-stream network simulation

This document explains how to build and simulate a network at the byte-stream level: connections that carry bytes in order, with partial reads and writes, delays, resets and backpressure. It is the design for stage 2 of `networkslice.md`. Stage 1 (message-level `Network`, both styles) is described there.

Status: design. Nothing here is implemented in the repo; an earlier prototype was deleted so that it can be rewritten from scratch. Statements about FoundationDB, TigerBeetle and tickloom come from reading their sources in this session (`fdbrpc/sim2.actor.cpp`, `flow/include/flow/network.h`, `src/testing/packet_simulator.zig`, `src/message_bus.zig`, `tickloom/network/*`) and are marked as such. Everything else is our design.

## 1. Purpose

We want a network layer that:

1. Can run on real sockets (Java NIO) and in a deterministic simulation, with the same code above the connection.
2. Exercises the real framing, buffering and reconnect code under simulated faults, instead of replacing them with a message-level shortcut.
3. Shows how an FDB-style event queue and a tick-driven loop both drive the same connection API.

## 2. Why simulate at the byte level

A message-level simulator (tickloom's `SimulatedNetwork`, TigerBeetle's `packet_simulator`) delivers whole messages, with delay, loss, replay, partitions and clogging. It tests protocol logic well. It never runs the code that turns bytes into messages.

A byte-level simulator sits under the framing code, so that code runs for real against hostile byte streams.

| Level | Faults | Bugs it can find |
|---|---|---|
| Message | delay, loss, reordering, replay, partitions, clogging | protocol bugs: stale leaders, lost or reordered messages, bad retry logic |
| Byte stream | delayed bytes, partial reads and writes, close or reset mid-frame, full send buffers, connect failures | transport bugs: a frame split across reads, two frames in one read, a connection dying mid-frame, ordering across reconnects, backpressure and drop policy |

Consensus code depends on the first row, because it uses timeouts and retries and does not rely on connection state. Transport code depends on the second. Both simulators can coexist behind the same `Network` interface.

## 3. Where it sits

```
Process / Replica             take a Network; they cannot drive it
Network                       send(Message), registerMessageDispatcher(...)          (stage 1)
Transport layer               Frame, FrameReader, MessageCodec, per-peer outbound queue, reconnect
Connection / Listener         byte streams: read, write, onReadable, onWritable, accept, connect   <- this document
  real:  NIO SocketChannel + Selector
  sim:   SimConnection, delivery tasks on a queue
```

Tickloom's `NioConnection` mixes the bottom two layers: it holds the `SocketChannel` and `SelectionKey`, and decodes frames inside `read()`. To plug a simulated stream underneath the framing code, that code must be extracted from `NioConnection` into the transport layer. This is part of stage 2.

## 4. Vocabulary

- **Address:** a `host:port` record. The simulator never touches the OS and uses it as a key.
- **Transport:** creates listeners and outbound connections.
- **Listener:** yields incoming connections, one `accept()` at a time.
- **Connection:** one end of a bidirectional byte stream.
- **Driver:** whoever runs the loop: it advances time, polls for I/O, and runs due tasks.
- **Scheduler:** `schedule(delay, task)` and `post(task)`. It is how an implementation hands work to the driver. A completion always goes through it, so a future never completes inside the call that returned it.
- **Pipe:** one direction of a simulated connection. A connection is two pipes.

## 5. The API

```java
interface Transport {
    Listener listen(Address on);
    TickCompletableFuture<Connection> connect(Address to);
}

interface Listener {
    Address address();                            // the address actually bound (port 0 picks a free one)
    TickCompletableFuture<Connection> accept();   // the next incoming connection
    void close();                                 // a pending accept fails, and the backlog is aborted
}

interface Connection {
    int read(ByteBuffer dst);                     // bytes available now; 0 = none yet; -1 = end of stream
    int write(ByteBuffer src);                    // bytes accepted into the outbound buffer; 0 = buffer full
    TickCompletableFuture<Void> onReadable();     // use after read returned 0
    TickCompletableFuture<Void> onWritable();     // use after write returned 0
    void close();                                 // clean end: bytes written so far are delivered, then EOF
    void abort();                                 // reset: in-flight bytes are dropped
}
```

This is FoundationDB's `IConnection` shape (`flow/include/flow/network.h`): non-blocking `read(begin, end)` and `write(buffer, limit)` that return counts, possibly 0, plus `onReadable()` and `onWritable()` futures to wait on after a 0. FDB signals a dead connection by throwing; we add `-1` for a clean end of stream.

## 6. The contract

**Data**
1. Within one direction of one connection, bytes arrive in the order written, each exactly once. Nothing is guaranteed across connections.
2. `write` consumes from `src.position()` and advances it by the returned count, like NIO. The caller keeps the rest and retries. The implementation copies the accepted bytes before returning.
3. `read` fills `dst` from its position and advances it by the returned count. It never returns more than `dst.remaining()`.
4. A byte count says nothing about message boundaries. One `write` can arrive as several reads, and several writes as one read.
5. `write` returning n means the bytes are in the outbound buffer. It does not mean they were delivered or read.

**Readiness**
6. If `read` returned 0, a later `onReadable()` completes when data arrives, the stream ends, or the connection dies. If `write` returned 0 and the connection is alive, a later `onWritable()` completes when room appears.
7. Futures never complete inside the call that returned them. They complete later, from the driver, on the loop thread. This stops the read and write loops from recursing, and it matches the file contract.
8. Calling `onReadable()` when data is already waiting is allowed. The future completes on the next driver pass.

**Lifecycle**
9. `close()` is a clean end. Bytes written before it are delivered before the peer sees `-1`. After `close()`, local `read` and `write` throw.
10. `abort()` is a reset. The local end fails at once: its pending readiness futures fail (through the scheduler) and its `read` and `write` throw `ConnectionResetException`. The peer end fails after a reset delay, by a scheduled task. Until that task runs, the peer can still read bytes that were already delivered, and bytes it writes are lost. When the peer end fails, its unread bytes are dropped.
11. `accept()` completes with connections in arrival order. Connections that arrive while no `accept()` is pending wait in the listener. Only one `accept()` may be pending.
12. `connect` fails with a connect exception if nobody listens at the address.

**Failure**
13. A failed connection is dead. The layer above reconnects. Bytes in flight when it died are lost, and the sender cannot tell which.
14. Nothing here detects a silent partition. A connection that stops delivering just stops. The transport layer needs idle timeouts or pings to notice.

**Threading and time**
15. All calls, and the driver, run on the loop thread. Time is whatever the driver says.

Implementation notes on the contract:
- A pending readiness future may complete or fail when the connection dies; the next `read` or `write` throws. The simulator fails it, and NIO completes it (the key becomes ready) and lets the `read` throw. Callers must handle both, and `ReadLoop` and `WriteLoop` do.
- A readiness future may complete when nothing has changed. The caller retries and may get 0 again.
- `write` returning 0 because of an injected partial-write fault is allowed: `onWritable()` completes at once whenever room exists, so the caller is never left waiting.

## 7. The simulation model

### 7.1 Time and the scheduler seam

A simulated `write` does not deliver anything; it schedules a delivery. `SimConnection` (and `SimTransport`) get a small scheduler and nothing else:

```java
interface Scheduler {
    long now();                                       // the scheduler's own time
    void schedule(long delay, Runnable task);         // run `task` after `delay` units of the scheduler's own time
    default void post(Runnable task) { schedule(0, task); }
}
```

The two styles supply it differently, and this is where the ownership difference from `networkslice.md` shows up at the byte level:

| | FDB style | Tick style |
|---|---|---|
| Scheduler | the shared `TaskQueue`, ordered by `(time, seq)`, also used by `FDBStyleFileIO` and every timer | the transport's own `(dueTick, seq)` queue, owned by the network |
| Unit of `delay` | queue time (milliseconds in a real loop, virtual time in a simulation) | ticks |
| Who advances time | the driver calls `advanceTo(now)`, with `now` from the clock or by jumping to `nextDueTime()` | `tick()` increments the tick counter and runs the tasks that are due |

The connection code is identical in both. It never knows which scheduler it has.

### 7.2 A connection is two pipes

```
client end                                server end
write ──► pipe C→S ──────────────────────► read
read  ◄── pipe S→C ◄────────────────────── write
```

Each pipe holds:

```
inFlight       queue of chunks: (bytes, deliverAt), or an EOF marker
delivered      bytes that arrived and have not been read (the receiver's read buffer)
capacity       limit on inFlight bytes + delivered bytes
lastDeliverAt  the latest delivery time scheduled so far
pending        at most one readable future (receiver) and one writable future (sender)
state          OPEN, CLOSED (clean), RESET
```

### 7.3 Write

```
write(src):
    if reset: throw ConnectionResetException; if closed locally: throw IllegalStateException
    room = capacity - (inFlightBytes + delivered.size())
    n = min(src.remaining(), room, faultLimit)       // faultLimit: see 8, "partial write"
    if n == 0: arrange a later onWritable completion (invariant I3); return 0
    chunk = copy n bytes from src; advance src.position by n
    deliverAt = max(lastDeliverAt, now + latency.sample())     // never overtake an earlier chunk
    lastDeliverAt = deliverAt
    inFlight.add(chunk, deliverAt)
    taskQueue.schedule(deliverAt - now, () -> deliverNext(pipe))
    return n
```

`deliverAt = max(lastDeliverAt, ...)` is the same trick as the per-file FIFO in the planned `SimulatedFileIO`: random latencies must not reorder a stream.

### 7.4 Delivery task

```
deliverNext(pipe):
    if pipe is RESET: return                              // dropped
    if pipe is partitioned: pipe.parked = true; return    // leave the chunk in inFlight; see below
    chunk = inFlight.poll()                               // always the head, so order holds
    if chunk is EOF: pipe.eofDelivered = true
    else: delivered.append(chunk.bytes)
    if receiver has a pending readable future: post(complete it); clear it
```

**Partitions and clogs.** `deliverAt` is fixed when `write` runs, but a partition can start afterwards. A delivery task that finds its pipe partitioned does not deliver. It marks the pipe parked and returns, and the chunk stays at the head of `inFlight`. A `write` on a parked pipe still adds its chunk to `inFlight` (capacity still applies, so backpressure builds) but schedules no task. When the partition heals, or a timed clog expires, `release(pipe)` schedules one delivery task per chunk in `inFlight`, with `deliverAt = max(lastDeliverAt, now + latency.sample())` each time, in order, and updates `lastDeliverAt`. Every later write is then scheduled behind the released chunks, so invariant I1 holds across a partition. A partition that never heals leaves the pipe parked forever, which is the hang described in the limits.

### 7.5 Read

```
read(dst):
    if reset: throw ConnectionResetException
    if delivered is not empty:
        n = min(dst.remaining(), delivered.size())
        move n bytes to dst; advance dst.position
        if sender has a pending writable future and room now exists: post(complete it); clear it
        return n
    if eofDelivered: return -1
    return 0                                              // caller waits on onReadable()
```

### 7.6 Readiness futures

`onReadable()` creates a future and remembers it as the pipe's pending readable. If data or EOF is already waiting, the future completes through `post` on the next pass. `onWritable()` is symmetric. Because the contract allows only one pending future of each kind per end, a second call while one is pending is a bug and should throw.

### 7.7 Backpressure

Capacity counts bytes in flight plus bytes delivered and not yet read. A reader that stops reading eventually fills the pipe, `write` returns 0, and the sender waits on `onWritable()`. This reproduces FDB's `sendBufSize - (writtenBytes - receivedBytes)` accounting and gives the transport layer's outbound queue something real to push against.

### 7.8 Close and abort

- **Clean close.** `close()` appends an EOF marker to the sender's `inFlight`, scheduled like any other chunk, so all earlier bytes are delivered first. The peer sees `-1` after reading everything. Local `read` and `write` throw after `close()`.
- **Peer writes after EOF.** Once the peer's EOF has been delivered, the other end's writes throw `ConnectionResetException` (broken pipe). This is the simplest rule and an open question (section 14).
- **Abort.** `abort()` fails the local end at once: its pipes are marked RESET, its pending readiness futures fail with `ConnectionResetException` (posted through the scheduler), and its `read` and `write` throw. The peer end fails later: a task scheduled after the reset delay marks the peer's pipes RESET, drops `inFlight` and `delivered`, fails the peer's pending readiness futures, and makes its `read` and `write` throw. Delivery tasks already scheduled find a RESET pipe and do nothing. Invariant I5 holds per end, from the moment that end is RESET.

### 7.9 Listen, accept and connect

A `SimNetwork` object keeps a map from `Address` to `SimListener`. Each host gets a `SimTransport` from `network.transport(host)`; partitions and clogs are between hosts. Connecting to an address nobody listens on, or across a partition, fails with `ConnectException` after the connect delay.

```
connect(to):
    f = new future
    listener = map.get(to)
    if listener == null: schedule(connectDelay, () -> f.fail(ConnectException)); return f
    (clientEnd, serverEnd) = new connected pair
    schedule(connectDelay, () -> { f.complete(clientEnd); listener.arrive(serverEnd); })
    return f

listener.arrive(serverEnd):
    if an accept is pending: complete it (through post)
    else: backlog.add(serverEnd)

accept():
    if backlog is not empty: return a future completed through post with backlog.poll()
    else: pending = new future (one at a time)
```

FDB splits this into `connectHandshake` and `acceptHandshake`, each a random delay (`delay(0.01 * random01())` in `sim2.actor.cpp`). We can start with one delay and split later.

### 7.10 Determinism

All randomness comes from a seeded `Random`, with a separate stream for latency and for faults, so adding a fault does not change the latency schedule. The same seed gives the same sequence of deliveries, resets and partial writes. A failing test prints its seed.

## 8. Fault catalogue

| Fault | How it is simulated | What it models | Bug it finds |
|---|---|---|---|
| Latency | `now + sample` per chunk, order kept by section 7.3 | network delay | timeouts that assume instant delivery |
| Partial delivery | split a chunk into pieces with increasing `deliverAt` | segmentation, small reads | a frame split across reads |
| Partial write | `n = min(n, random(0, limit))`, possibly 0 | full socket buffer | callers that assume `write` takes everything |
| Small capacity | tiny pipe capacity | slow reader | missing backpressure handling, unbounded queues |
| Clog | extra delay on a pair until time T | congestion | timeouts, slow-peer handling |
| Partition | deliveries on a pair postponed to the heal time, or forever | cut link | hung connections, missing idle timeouts |
| Random reset | on each `read` or `write`, abort with probability p | peer crash, `RST` | cleanup, reconnect, lost in-flight messages |
| Connect delay or failure | delay before the connect future completes, or fail it | slow or refused connect | connect timeout, retry backoff |
| Bit flip (later) | flip a byte in a delivered chunk | corruption | missing checksums in the framing |

What the references do (from the source):
- **FDB `Sim2Conn`:** `writtenBytes` becomes `sentBytes` after a short random delay, then `receivedBytes` after a send and receive delay from `g_clogging`, and the receive position can land anywhere between received and sent. Under `BUGGIFY`, `toSend = min(toSend, randomInt(0, 1000))`. `rollRandomClose()` runs at the start of every `read` and `write`. The comment on `isStableConnection()` in `flow/include/flow/network.h` says `Sim2Conn` uses it to disable bit flips for connections between a parent and child process, which implies bit flips are on for other connections; we have not read that code.
- **TigerBeetle `packet_simulator`:** per-path delay (mean and minimum), loss and replay probabilities, partition modes (`uniform_size`, `uniform_partition`, `isolate_single`), symmetric or asymmetric, path clogging with an exponentially distributed duration, and `path_maximum_capacity` after which packets are dropped. It works on packets, not bytes.
- **Tickloom `SimulatedNetwork`:** a tick counter, a `(tick, seq)` priority queue, exponential delay, per-link loss, clogging, partitions that come and go, and fault rules per message type. It checks the partition at send time.

## 9. Invariants the simulator must keep

If the simulator breaks these, a test failure may be the simulator's fault.

- **I1.** Within one direction, bytes are delivered in write order, none duplicated, none invented (unless a corruption mode is on).
- **I2.** If `read` returned 0 on a live connection, a later `onReadable()` completes when bytes arrive or fails when the connection dies.
- **I3.** If `write` returned 0 on a live connection, a later `onWritable()` completes. This includes the case where a fault, not a full buffer, caused the 0.
- **I4.** Futures complete only from the driver, never inside the call.
- **I5.** Once an end is RESET, no byte is delivered to it and no byte is readable from it.
- **I6.** After a clean close, every byte written before it is delivered before the peer sees `-1`.
- **I7.** The same seed gives the same schedule.

## 10. The real NIO implementation

| Contract | NIO |
|---|---|
| `read` | `SocketChannel.read(dst)`, which gives -1 at EOF and 0 when nothing is available |
| `write` | `SocketChannel.write(src)`, which gives 0 when the socket buffer is full |
| `onReadable()` | register `OP_READ`, complete the future when the key is ready, then clear the interest |
| `onWritable()` | register `OP_WRITE`, complete when ready, then clear the interest |
| `accept()` | register `OP_ACCEPT` on the `ServerSocketChannel`, call `accept()` when ready |
| `connect` | non-blocking `connect`, then `OP_CONNECT` and `finishConnect()` |
| backlog | the OS accept backlog, with `OP_ACCEPT` interest cleared while no `accept()` is pending |
| reset | `IOException` from `read` or `write`, rethrown as `ConnectionResetException` |

The `poll` step turns ready keys into posted completions:

```java
void poll(long timeoutMs) throws IOException {
    if (timeoutMs <= 0) selector.selectNow(); else selector.select(timeoutMs);   // select(0) blocks forever
    for (SelectionKey key : selector.selectedKeys()) {
        if (key.isValid()) ((Ready) key.attachment()).onReady(key.readyOps());
    }
    selector.selectedKeys().clear();
}
```

Gotchas:
- Set interest only while waiting, and clear it when served, or `select` returns immediately forever. This applies to `OP_CONNECT` too.
- `finishConnect()` can return false; keep `OP_CONNECT` and retry.
- A cancelled key throws if touched, so check `isValid()`.
- Partial `write` is normal. The writer loop owns the retry.
- If the loop sleeps in `select`, a completion posted from another thread (the file's I/O thread) needs `selector.wakeup()`. Sockets alone need none.

Tickloom's `NioNetwork.processSelectedKeys` takes the other approach: the key handler calls `connection.read()` and `connection.write()` inline, and the framing runs inside `NioConnection`. Here, a key only completes a future, and the read and write calls happen in the loops that wait on those futures. That is what lets `SimConnection` replace it.

## 11. Driving it

The only seam between the two styles is the `Scheduler` from section 7.1. The real NIO classes use only its `post`.

```java
// FDB style:  scheduler = the shared TaskQueue            (delay in queue time)
// tick style: scheduler = the transport's own tick queue  (delay in ticks)
```

**FDB-style real loop**
```java
while (running) {
    long now = clock.millis();
    long wait = tasks.nextDueTime().map(t -> Math.max(0, t - now)).orElse(IDLE_MS);
    transport.poll(wait);              // ready keys become posted tasks
    tasks.advanceTo(clock.millis());   // run what is due: completions, loops, timers
}
```
`TaskQueue.nextDueTime()` does not exist yet.

**FDB-style simulation loop**
```java
while (!done) {
    tasks.advanceTo(tasks.nextDueTime().orElseThrow());   // time jumps to the next event
}
```

**Tick style**
```java
void tick() {
    transport.poll(0);          // real NIO: ready keys post completions to the tick queue; the simulator has no poll
    tickCounter++;
    runDueTasks(tickCounter);   // run tasks with dueTick <= tickCounter, in (dueTick, seq) order
}
```
The network owns the tick queue and the counter. Nothing else shares them.

## 12. Using it: a server

The server is a set of callback loops over the API. The same code runs on NIO and on `SimConnection`.

```java
class EchoServer {
    void start() { acceptLoop(); }
    private void acceptLoop() {
        listener.accept().whenComplete((conn, error) -> {
            if (error != null) return;             // listener closed
            new Session(conn).start();
            acceptLoop();                           // keep accepting
        });
    }
}

class Session {
    private final Connection conn;
    private final Scheduler scheduler;              // injected; the loops use only scheduler.post(...)
    private final ByteBuffer in = ByteBuffer.allocate(4096);
    private final ArrayDeque<ByteBuffer> outbound = new ArrayDeque<>();
    private boolean writing;

    void start() { readLoop(); }
    private void readLoop() {
        try {
            for (int i = 0; i < MAX_READS_PER_TURN; i++) {          // fairness: do not hog the loop
                in.clear();
                int n = conn.read(in);
                if (n < 0) { conn.close(); return; }                // peer closed
                if (n == 0) {                                        // nothing yet
                    conn.onReadable().whenComplete((v, e) -> { if (e != null) fail(e); else readLoop(); });
                    return;
                }
                in.flip();
                send(copyOf(in));                                    // echo; a real server frames and decodes here
            }
            scheduler.post(this::readLoop);                          // more data waiting: reschedule
        } catch (RuntimeException e) { fail(e); }
    }
    void send(ByteBuffer bytes) { outbound.add(bytes); if (!writing) writeLoop(); }
    private void writeLoop() {
        writing = true;
        try {
            while (!outbound.isEmpty()) {
                ByteBuffer head = outbound.peek();
                int n = conn.write(head);
                if (!head.hasRemaining()) outbound.poll();
                if (n == 0) {
                    conn.onWritable().whenComplete((v, e) -> { if (e != null) fail(e); else writeLoop(); });
                    return;
                }
            }
            writing = false;
        } catch (RuntimeException e) { fail(e); }
    }
    private void fail(Throwable e) { conn.abort(); outbound.clear(); }   // only this connection dies
}
```
A client is the mirror image: `connect`, then the same two loops. This is also the inbound and outbound half of the transport layer: accept loop, read loop, then `FrameReader`, `codec.decode`, `dispatcher.onMessage`; and `connect`, then the write loop fed by `send(Message)`.

Each `onReadable()` or `onWritable()` returns a fresh future with one callback, which fits the single-callback `TickCompletableFuture`.

## 13. Testing strategy

1. **Contract tests over a fixture.** `ConnectionFixture` with `Transport transport()` and `void driveOnce()`. The same tests run on `SimTransport` and `NioTransport`: ordering, `read` returning 0 and -1, backpressure, clean close and reset, one pending `accept()`, connect to a dead address.
2. **Scenario tests on the simulator.**
   - One-byte deliveries: the echo still returns the exact bytes.
   - Random partial writes: the writer loop sends everything.
   - A reset while a frame is half-sent: only that session ends, and the server keeps accepting.
   - A tiny capacity: the writer waits on `onWritable()`, and no queue grows without bound.
3. **Byte conservation property test.** A writer sends random-size chunks, a reader loop collects. With every fault on except reset and corruption: collected equals written. With reset: collected is a prefix of written. The seed is printed on failure.
4. **Invariant checks.** The simulator asserts I1 to I7 internally, so a bug in it fails loudly.
5. **Same test on two drivers.** The FDB-style jump-to-next-event loop and the tick-style loop run the same scenario and must agree on the bytes delivered.
6. **Partition while bytes are in flight.** Write several chunks, start a partition before they are delivered, write more, then heal. All bytes arrive in order (I1), and during the partition the pipe fills and `write` returns 0.

## 14. Known limits and open questions

- **What is not simulated:** TCP congestion control, Nagle, MTU, half-open detection, kernel buffers beyond `capacity`, DNS, ports, and TLS.
- **Writes after the peer closed.** Real TCP accepts a few and then resets. Rule for now: after the peer's EOF is delivered, writes throw. Refine if needed.
- **Silent partitions.** A partition that never heals leaves a connection that hangs forever. The transport layer needs idle timeouts or pings, and a test that a partition is noticed within a bound.
- **Backlog limit.** Unbounded in the simulator, bounded by the OS in NIO. Add a limit and refuse beyond it.
- **Half-close.** `shutdownOutput` is not modelled.
- **Corruption.** Needs a checksum in the framing first; add it with the bit-flip fault.
- **`peerAddress()`** on `Connection`. FDB has `getPeerAddress()`; add when the transport layer needs it.
- **One connection per peer or two.** One outbound connection per peer for sending, with accepted connections used for receiving, avoids simultaneous-connect races. Tickloom's `NioNetwork` suggests this with its `getOrCreateOutboundChannel`; confirm when the transport layer is written.

## 15. Build order

1. Extract `TaskQueue` to a top-level class, with `advanceTo(now)` and `nextDueTime()`, and define the `Scheduler` seam it implements.
2. One simulated pipe pair at zero latency, built directly. Test: write 5 bytes, drive, read 5 bytes.
3. Backpressure: capacity, `write` returning 0, `onWritable()`.
4. Clean close and abort.
5. Latency with order preserved (section 7.3).
6. `SimTransport`: listen, accept, connect, backlog, refused connect.
7. Echo server and client over the simulator.
8. Faults, seeded: partial delivery, partial write, small capacity, clog, partition, random reset, connect failure.
9. Contract tests over `ConnectionFixture`.
10. `NioTransport`, in pieces: listener and accept on loopback, then connect with `finishConnect`, then read and write with readiness, then close and errors, then the `poll` loop. The contract tests from step 9 must pass.
11. The tick-style scheduler (own `(dueTick, seq)` queue), and the same scenarios on both drivers.
12. Transport layer: `FrameReader`, outbound queue, reconnect, then `FDBStyleNetwork` over connections.
