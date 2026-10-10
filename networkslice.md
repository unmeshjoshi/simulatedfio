# Network slice: message-level network, FDB style and tick style

Goal: show how a delivery is scheduled in the two styles, behind one `Network` interface that `Process` and `Replica` use.
Stage 1 is message level only. The byte-stream `Connection` (framing, partial reads, NIO) is stage 2 and is still to be designed.

```
Process / Replica      take a Network; they cannot drive it
Network                send(Message), registerMessageDispatcher(...)         (no tick)
TickableNetwork        Network + tick()                                      tick style
FDBStyleNetwork        Network, driven by TaskQueue.advanceTo(now)           FDB style
```

Packages: new code goes in `simfio.net`, and moves to tickloom when ported, as with the file I/O.

## How the two styles differ at this level

| | FDB style (`FDBStyleNetwork`) | Tick style (`SimulatedNetwork`) |
|---|---|---|
| Who owns the queue and the clock | the shared `TaskQueue`, driven from outside | the network itself |
| What `send` does | `taskQueue.schedule(latency, runnable)` | adds a record `(dueTick, seq, message)` to its own queue |
| Driving call | `taskQueue.advanceTo(now)` | `network.tick()` |
| Ordering | `(time, seq)` | `(dueTick, seq)` |
| Time | an input, chosen by the driver | an implicit unit, one tick |
| What a delivery is | any runnable | a message record |
| Faults checked | at send, and optionally at delivery | at send |

The logic is nearly the same. The difference is ownership: the FDB-style network adds runnables to a queue shared with the file, the timers and everything else, and the tick-style network owns its queue and clock.

## Decisions this slice relies on
- `send(Message)` never blocks and returns no future. The caller retries on its own timeout, for example through `RequestWaitingList`.
- Within one source/destination pair, messages arrive in `(time, seq)` order unless a fault rule reorders them. Nothing is guaranteed across pairs.
- A message can be lost (loss rule, partition). The network never duplicates a message; duplicates come only from the caller's retries.
- Delivery happens on the loop thread, from the driver's call (`advanceTo` or `tick`), never from inside `send`.
- `Process` and `Replica` take a `Network`, so they cannot call `tick()` on it. The loop owner drives it.
- All randomness comes from a seeded `Random`, with a separate stream for latency and for faults, so a failure can be replayed from its seed.
- `Process` and `Replica` stay tick-based. The FDB-style driver bridges with a repeating task that calls `process.tick()` and reschedules itself.

## Prerequisites
- `TaskQueue.advanceTo(now)` (thinslice.md task 18).
- `FileFixture`-style `driveOnce()` idea (thinslice.md task 19), reused here as `NetworkFixture`.

## Tasks

1. **Define the interfaces.** `Network` (`send`, `registerMessageDispatcher`, `close`), `TickableNetwork extends Network` with `tick()`, `MessageDispatcher` with `onMessage(Message)`. No `bind` method in the base type.
2. **Minimal `Message`.** A record with source `ProcessId`, destination `ProcessId`, a type, and a `byte[]` payload. `ProcessId` already exists in this repo.
3. **`FDBStyleNetwork`.** Constructor takes the `TaskQueue`, a seeded `Random`, and a latency range in queue-time units. `send` schedules a delivery runnable with a sampled latency. The runnable calls the registered dispatcher for the destination.
4. **FDB-style faults.** Loss probability, per-link partitions that can be set and cleared at any time, and clogging of a link until a given time. The delivery runnable re-checks the partition at run time, so a partition that starts while a message is in flight drops it.
5. **Tick-style `SimulatedNetwork` behind `TickableNetwork`.** Port a trimmed version of tickloom's test-side `SimulatedNetwork`: tick counter, `(dueTick, seq)` queue, exponential delay, loss, partitions, clogging. Check the partition at send, as tickloom does.
6. **Network fixture.** `NetworkFixture` with `Network network()` and `void driveOnce()`: `network.tick()` for the tick style, `taskQueue.advanceTo(clock)` for the FDB style. The FDB-style fixture has a virtual-time mode where the test chooses `now`.
7. **Shared network contract tests.** Over `NetworkFixture`, run on both implementations:
   - a message arrives at its destination and at no other;
   - messages between one pair arrive in send order when latency is constant;
   - a partitioned link delivers nothing, and delivery resumes after it heals;
   - a loss probability of 1.0 delivers nothing, and of 0.0 delivers everything;
   - the same seed gives the same arrival order and the same losses.
8. **In-flight partition test.** Send a message, start a partition before its delivery time, and check that the FDB-style network drops it. Run the same scenario on the tick-style network and record the difference, since it checks only at send. Decide whether the tick-style one should also check at delivery.
9. **Tick adapter for the FDB-style stack.** A repeating task on the `TaskQueue` that calls a `Tickable`'s `tick()` once per millisecond of queue time and reschedules itself. A test shows a `RequestWaitingList` timeout firing at the right queue time.
10. **Two-process test on each style.** Two toy processes send a request and a reply over the network, with a timeout and a retry. The same test body runs on both fixtures. It shows that `Process` code does not change between the styles.
11. **Responsibility comments.** A short class comment on `Network`, `TickableNetwork`, `FDBStyleNetwork` and `SimulatedNetwork` saying what each owns and what it leaves to the caller.

## Done when
- The shared contract tests pass on both implementations.
- The two-process test reads the same on both and differs only in the fixture.
- Nothing above `Network` knows which style it runs on.

## Stage 2: byte-level network
Not part of this slice. The design is written up in `bytestream-network.md` (API, contract, simulation model, fault catalogue, NIO mapping, drivers, tests, build order). The questions it answers:
- What is the `Connection` API: FDB's `IConnection` (non-blocking `read`/`write` plus `onReadable`/`onWritable`) or a future-based `read`/`write` like `FileIO`?
- How does it serve both styles? The FDB style drives it with scheduled tasks; the tick style drives it with `tick()`.
- Where do framing, the per-peer outbound queue, and reconnect live: a transport layer between `Network` and `Connection`.
- Real implementation: NIO `SocketChannel` and `Selector`, with interest registered only while waiting, and `selector.wakeup()` when the file's I/O thread posts to a shared `TaskQueue`.
- Simulated implementation: `SimConnection` with delivery delay, partial reads and writes, random close, connect delay, and a bounded send buffer.
- Reuse tickloom's `Frame`, `FrameReader` and `MessageCodec`, and extract the frame handling out of `NioConnection`.

## Not in this slice
Byte streams, framing, NIO, reconnect logic, backpressure on the outbound queue, TLS, and the port to tickloom.
