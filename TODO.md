# TODO: deterministic simulation of file I/O and network, FoundationDB style vs TigerBeetle style

Learning project. Everything is written from scratch except `TickCompletableFuture`, which is the only code kept.
The plan is four stages, in this order:

1. FoundationDB style: simulated and real file I/O
2. TigerBeetle style: simulated and real file I/O
3. FoundationDB style: simulated and real network
4. TigerBeetle style: simulated and real network

Each step ends with a test and a short "Insight" note.

## 0. What the two reference code bases do (verified in `~/work/foundationdb` and `~/work/tigerbeetle`)

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

## 1. Decisions made

- **Task is a closure** (`Runnable`), not a promise completion. A completion task is `() -> future.complete(x)`.
  Optionally add a `delay(t)` helper that returns a future, for `choose`-style code.
- **Queue shape for the FDB style:** one heap ordered by `(due time, seq)`, plus a thread-safe inbox. No separate ready
  queue and no priorities. `seq` is required, because Java's `PriorityQueue` is not stable.
- **No random tiebreak** (FDB does not have one). Same-time tasks are FIFO.
- **`now` is not stored in the queue.** The driver passes it in. Inbox tasks are stamped with `now` when the loop
  drains them, not when they are submitted.
- **Entry points:** `schedule(delay, task)` is loop-thread only. `submit(task)` is the only thread-safe call.
- **Cancellation lives on the future:** `cancel(reason)` fails the future if it is still pending and runs abandon hooks the
  producer registered. This replaces the two-future wrapper.
- **TigerBeetle style, first cut:** each component's `tick()` advances its counter and loops until nothing is due. Split into
  `tick()` and `step()` only when a test needs to stop between two ops (see 2.7).
- **Contract tests are style-neutral.** The same file I/O tests run against all four implementations.

## 2. Stage 1: FoundationDB style, file I/O

### 2.1 Queue and drivers
- [ ] `Scheduler`: `now()`, `schedule(delay, task)`, `submit(task)`.
- [ ] `TaskQueue`: heap by `(time, seq)` and a thread-safe inbox. `addTimer`, `drainInbox(now)`, `runDue(now)`, `nextTimerTime()`.
- [ ] Thread-confinement assertion on `schedule` and `runDue`, none on `submit`.
- [ ] Sim driver: jump `now` to the next timer, run due tasks, repeat until idle.
- [ ] Real driver: drain the inbox, run due timers by `System.nanoTime()`, block until the next timer or a `submit` when idle.
- [ ] `runUntil(done, limit)`: in sim it fails at once with "nothing left to run" if the queue goes idle first. In prod it times out.
- Tests: timers fire in time order; same-time tasks are FIFO; a task that schedules a zero-delay task runs it in the same
  drain; in the real driver a `submit` from another thread wakes a blocked loop and tasks run only on the loop thread.

### 2.2 File I/O
- [ ] `FileIO` interface: positional `write(data, offset)` and `read(offset, length)` returning `TickCompletableFuture`.
- [ ] `SimulatedFileIO`: applies the bytes and completes the future in one task scheduled with a seeded latency.
- [ ] `RealFileIO`: single I/O thread for the blocking work; it posts completions with `submit`, and the future is only
      ever completed on the loop thread.
- [ ] Shared contract tests (write returns count, gap reads as zeros, short read at end, empty read past end, results
      arrive in the order issued, nothing completes until the loop runs) run against both.
- [ ] Same seed gives the same trace; two seeds can differ.

### 2.3 Timeouts and cancellation
- [ ] `TickCompletableFuture`: add `cancel(reason)` and an `onCancel(Runnable)` hook; decide whether `cancel` forwards
      through `thenApply` / `thenCompose` (suggest: no, cancel the future the producer created).
- [ ] `TimedFileIO`, written once against `Scheduler`: `schedule(timeout, () -> future.cancel(new RequestTimeoutException(...)))`.
- [ ] `SimulatedFileIO`: a queued op skips applying if its future was cancelled before its ready time.
- [ ] `RealFileIO`: `onCancel` sets a shared `AtomicBoolean` the worker checks before the syscall. The worker never reads
      the future, and its completion checks `isPending()` before completing.
- [ ] Decide and test whether a timed-out write may still apply (default: yes, if it has already started).
- Tests: completes before the timeout (late timer is a no-op); times out at exactly `t = timeout`; equal-time case is
  FIFO by scheduling order (get the other outcome by changing latency or timeout); stale timer keeps the sim queue
  non-idle until it fires; cancel before the ready time prevents the write.

*Insight to record:* what a stale timer costs in the sim iteration count; what "cancel" can and cannot stop in real I/O.

### 2.4 Faults and seeds
- [ ] Separate PRNG streams for latency and faults, so adding a fault does not change other draws.
- [ ] Inject short writes, latency spikes with a heavy tail, and operations that never complete.
- [ ] Seed sweep over about 1,000 seeds checking: exactly one outcome per request, nothing pending at idle, same seed
      gives the same trace. Print the seed on failure.

## 3. Stage 2: TigerBeetle style, file I/O

- [ ] 3.1 `Tickable { void tick(); }`.
- [ ] 3.2 `Timeout` as in `vsr.zig`: `after`, `ticks`, `ticking`; `start/stop/reset/tick/fired`; `tick()` counts only while
      ticking; assert the caller acts on `fired()` by stopping or resetting.
- [ ] 3.3 `TickDisk`: its own tick counter and its own op queue (ordered by ready tick, seeded latency). `tick()` does
      `ticks++`, then loops until nothing is due: pop the earliest due op, apply it, complete its future, repeat.
      Re-read the queue after every callback, because a callback may start a zero-latency op that must complete in
      the same tick.
- [ ] 3.4 The requester keeps a waiting list of `{future, Timeout}`. Its `tick()` ticks each timeout and, for any that
      fired, removes the entry and fails or cancels the future. A completion removes the entry too.
- [ ] 3.5 `TickLoop`: `while (!idle) { for (c : components) c.tick(); }` in a fixed order.
- [ ] 3.6 Real version: `tick()` advances the clock and timeouts; a poll call collects finished I/O (the same I/O thread
      and inbox, or NIO) and runs callbacks until none are left, bounded by the tick duration.
- [ ] 3.7 Run the same contract tests as stage 1, plus the timeout cases.
- Tests: swapping the tick order of disk and requester changes the equal-time outcome; a callback starting a zero-latency
  read completes it in the same tick; a timeout with `after=3` fires on the third tick and not before.

*Insight to record:* what idle time costs; tick order replacing FIFO as the source of determinism; who owns the pending state.

### 3.8 When needed: split `tick()` and `step()`
- [ ] `tick()` advances time only; `step()` runs one due op and returns whether it ran. Needed when a test must stop
      between two ops due on the same tick (a crash between two writes, or applied but not yet acknowledged), and to
      advance time without draining. In production `step()` maps to the poll-and-dispatch call.

### 3.9 Variation: retry instead of cancel
- [ ] A fired timeout resends with exponential backoff and jitter from the seeded PRNG, keeps the entry pending, and gives up
      after N attempts. Positional writes are idempotent; think about what would break for append.

## 4. Compare file I/O

- [ ] Same scenario under both styles: align one tick with one time unit and compare traces.
- [ ] Slow disk (latency up to 1,000,000): count loop iterations. Expect a handful for FDB and about 1,000,000 for ticks.
- [ ] Add a long timeout and see how the stale timer affects the FDB iteration count.
- [ ] Write the findings in `NOTES.md`: where the styles disagree and why.

## 5. Stage 3: FoundationDB style, network (to detail when stage 2 is done)

- [ ] Simulated connection: delivery as delayed tasks on the same queue, with seeded send and receive delays.
- [ ] Real network on the real driver (NIO selector or threads feeding the inbox).
- [ ] Contract tests: ordering per connection, partial reads, a peer going away, loss and delay.
- [ ] Request timeout over the network reusing `cancel` and `TimedFileIO`'s pattern; late reply after a timeout is ignored.
- [ ] Look at FDB's clogging model (`g_clogging`) for pair-wise delays.

## 6. Stage 4: TigerBeetle style, network (to detail later)

- [ ] A tickable packet-simulator component with its own typed queue; a real counterpart with tick plus poll.
- [ ] Same contract tests and timeout cases as stage 3.
- [ ] Compare with stage 3: typed per-component queues against the generic queue, for connections.

## 7. Stretch

- [ ] Priorities in the queue (FDB `TaskPriority`).
- [ ] Owner tag on tasks so a dead node's tasks can be dropped (FDB's `machine`).
- [ ] Durability: `sync`, and a crash that drops writes that completed but were not synced.
- [ ] Per-component clock skew (TigerBeetle's `time_sim`) against FDB's single clock.

## Questions each step should answer
- Who owns the pending state: the component, or the queue?
- What decides the order of events at the same time: tick order, or FIFO by scheduling order?
- What does idle time cost?
- What happens when a completion and a timeout land at the same time?
- Is the loop still deterministic if a callback schedules or ticks more work?
- Can the same code run on the sim and the real driver without knowing which?
