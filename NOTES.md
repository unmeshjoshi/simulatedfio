# Notes: FoundationDB style vs TigerBeetle style

Reference facts and the questions each step should answer. The task plans are in `thinslice.md` (file I/O, Wal, BatchedLog),
`networkslice.md` (message-level network) and `bytestream-network.md` (byte-stream network design).

## Order of work
1. Implement `FileIO`, `FileSystem` and their simulated versions here, following `thinslice.md` (tasks 1 to 30).
2. Port it into tickloom.
3. Change tickloom to remove its existing storage, and give each process instance a `FileIO` and a filesystem.
4. Write the storage section of the DST article, then the rest of the article changes.
5. Byte-stream network, redone from scratch (`bytestream-network.md`).

## What the two reference code bases do (verified in `~/work/foundationdb` and `~/work/tigerbeetle`)

| | FoundationDB | TigerBeetle |
|---|---|---|
| Queue | one generic task queue; components only add tasks to it | no global queue; each component owns typed queues (`Storage` has `reads`, `writes` ordered by `ready_at`, and a `next_tick_queue`) |
| What a task is | `PromiseTask`: running it means `promise.send(Void())`, which resumes the actor waiting on it | an op record (a read or write with buffer and callback) that the owning component knows how to complete |
| Who drives | the loop pops the next task and runs it. It knows nothing about disk or network | the loop calls `tick()` on every component in a fixed order; each component pops from its own queues |
| Time | one virtual clock at the queue level (`Sim2::time`, a `double`) | a logical tick counter per component (`Storage.ticks`, replica clock) |
| Idle time | the clock jumps to the next timer | costs one iteration per tick |
| Simulated disk and network | `delay()` tasks on the queue (`SimpleFile`, `Sim2Conn`); nothing is polled | `Storage` / `PacketSimulator` components with their own queues |
| Production loop | `Net2::run` polls the OS reactor, then runs timers and ready tasks | `replica.tick(); io.run_for_ns(tick_ms)`; `run_for_ns` polls `kevent`/`io_uring` and drains completions |
| Timeout | `delay(t)` is a timer task; `timeoutError(f, t)` is `choose { f; delay(t) }`. When the timer wins, the loser is cancelled by dropping its last reference. The timer is not removed from the queue | `vsr.Timeout` counter (`after`, `ticks`, `start/stop/tick/fired`, jitter, backoff), ticked by its owner every tick. **Retries** on fire (`on_request_timeout` resends) rather than cancelling |

Other facts worth remembering:
- FDB's queue (`flow/include/flow/TaskQueue.h`) has three structures: `ready`, `timers`, and `threadReady` (thread-safe, the
  only part other threads may touch). `Net2` had them from the first public commit. The simulator used a **single
  heap** ordered by `(time, stable counter)` until 2022 (`c6c449d04` moved it onto the shared queue).
- FDB's simulator has **no random tiebreak**. Same-time tasks run in insertion order. Variety between seeds comes from
  random delays (latency jitter, buggified delays).
- In simulation nothing needs polling, because the program plays the disk and the network and knows when things
  complete. Polling or waiting on the OS is needed only in production loops.
- TigerBeetle splits `Storage.tick()` (only `ticks += 1`) from `Storage.step()` (completes one due op). The cluster calls
  `step()` repeatedly until nothing advances, so several ops can complete within one tick, even when a callback
  starts a zero-latency op.

## Still to do after the slice
Not covered by the three plan documents:
- `SimulatedFileIO` with seeded latency (the crash rule is in `thinslice.md` task 29).
- Fault injection: short writes, heavy-tailed latency spikes, operations that never complete. Separate PRNG streams for latency and faults.
- Seed sweep over about 1,000 seeds checking one outcome per request, nothing pending at idle, and the same seed giving the same trace. Print the seed on failure.
- Cancellation on the future (`cancel(reason)` with an abandon hook), if a timeout should stop a queued op instead of only failing the caller.
- Retry-instead-of-cancel timeout (TigerBeetle style): resend with backoff and jitter, and what breaks for append.
- Compare the styles on a slow disk (latency up to 1,000,000): loop iterations, and the cost of a stale timer in the FDB style.

## Questions each step should answer
- Who owns the pending state: the component, or the queue?
- What decides the order of events at the same time: tick order, or FIFO by scheduling order?
- What does idle time cost?
- What happens when a completion and a timeout land at the same time?
- Is the loop still deterministic if a callback schedules or ticks more work?
- Can the same code run on the sim and the real driver without knowing which?
