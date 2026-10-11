# Thin slice: file I/O, Wal and BatchedLog on a real file

Goal: one thin thread through all three layers, on a real temp file, to show what each layer owns. No simulator yet.
The simulator comes after this, so the same layers become testable under crashes and faults.

```
BatchedLog   policy:     when to sync; completes each caller's future once the sync succeeds
   Wal       mechanism:  record format, offsets, append, sync, durableOffset, recover; never syncs by itself
TimeBoundedFileIO        a call that takes too long fails with TimeoutException (counted in ticks)
SingleThreadedFileIO     real file on one I/O thread; calls run and complete in submission order
```

Packages: new code goes in `simfio.wal` for now and moves to the workshop when this is ported to tickloom.

## Decisions this slice relies on
- `append` completes when the bytes are written (visible to reads), not durable.
- `sync()` completes when everything appended before it is durable. The file runs calls in order, so one `sync()` covers all earlier writes, even ones whose futures have not completed yet.
- `Wal` never calls `sync` itself. `BatchedLog` (or a consensus replica) decides when.
- `nextOffset` is claimed when `append` is called, not when the write completes, so back-to-back appends get consecutive offsets.
- The record offset is its position (LSN). The `Wal` treats payloads as opaque.
- A failed or timed-out write or sync is fatal: the log is poisoned and must be recovered from disk.
- `Wal` has no `tick()`, and it takes a `FileIO`, so it cannot tick the file. The owner of the loop holds the `TickableFileIO`, ticks it, and passes the same object to `Wal` as a `FileIO`. A `Wal` on a file nobody drives just hangs, and the class comment says so.
- Two implementations share the `FileIO` contract. Tick-driven ones (`SingleThreadedFileIO`, `SimulatedFileIO`, `TimeBoundedFileIO`) are `TickableFileIO`. `FDBStyleFileIO` is a plain `FileIO` driven by its `TaskQueue`.
- No sector padding and no file header in this slice. The class comment states the assumption: a sector is written atomically.

## Tasks

1. **Split the file interface and fix the `Wal` stub.** Create `FileIO` with `read`, `write`, `sync`, `size`, `truncate` and `close`, and move the durability, ordering, buffer and size rules from the `TickableFileIO` comment onto it. `TickableFileIO extends FileIO` and adds only `tick()`, with the threading rule: call it from the loop thread, and futures complete only inside `tick()`. `Wal.java` takes a `FileIO` (it can't tick the file), has no unused imports, and the project compiles.
2. **Record codec.** `[length: 4][crc32c: 4][payload]` with the CRC over the length and the payload. Encode a list of payloads into one buffer, and decode one record from a buffer. Zero length is invalid, and a maximum record size (constructor argument, default 1 MB) rejects garbage lengths.
3. **Codec tests.** Round trip, a flipped payload bit, a flipped length bit, a truncated record, a zero length, and a length over the maximum. Decode reports "invalid" and never throws on bad bytes.
4. **`Wal.append(List<byte[]>)`.** Encode the whole batch first (it may throw), then `offset = nextOffset; nextOffset = offset + encoded.length`, where `encoded.length` is the total size of the encoded batch (every record's length field, CRC and payload), not the sum of the payload sizes. Then `file.write(encoded, offset)`. Return a future with `offset`, the position of the first record, completed when the write completes.
5. **`Wal.sync()` and `durableOffset()`.** Record `covered = nextOffset` at call time, call `file.sync()`, and on success set `durableOffset = max(durableOffset, covered)`. The returned future carries the durable offset.
6. **Failure handling.** A failed write or sync fails its own future and poisons the `Wal`. Later `append` and `sync` calls fail at once.
7. **`Wal.recover()`.** Read the file from offset 0 in chunks, decode records, and stop at the first invalid one. Return `Entry(offset, payload)` in order. Set `nextOffset` and `durableOffset` to the end of the last valid record. Then truncate the file to that offset and sync if anything followed it, and only then become `READY`. Without the cut, a record that survived a crash beyond a lost one can rejoin the log once a later append fills the gap.
8. **`Wal` state.** `NEW` until `recover()` completes, then `READY`, then `POISONED` on failure. `append` and `sync` before `recover()` are rejected.
9. **`Wal` tests on a stub file.** Use a manual `TickableFileIO` stub (like `ManualFileIO` in `TimeBoundedFileIOTest`) to check these:
   - three back-to-back appends reach the file at offsets 0, `len1` and `len1 + len2`;
   - `append` completes before any sync, and `durableOffset()` is unchanged;
   - `sync()` advances `durableOffset()` to the end of everything appended before the call, and not past later appends;
   - the file sees no `sync` unless the caller called `sync()`;
   - a failed sync poisons the log.
10. **`BatchedLog` skeleton.** It wraps a `Wal` and has `tick()`. `append(payload)` calls `wal.append` at once (the write goes out immediately) and queues `(offset, future)`. The returned future means "durable".
11. **`BatchedLog` flush policy.** Constructor argument `maxGroupWaitTicks`. `tick()` counts ticks since the oldest waiter was queued and, when it reaches the limit, calls `wal.sync()` once. A flush is the only thing that calls `wal.sync()`, and at most one is in flight.
12. **`BatchedLog` completion.** When a sync succeeds, complete every waiter whose offset is at or below the returned `durableOffset`, in order. If the sync or any write fails, fail every waiter with that error and refuse later appends.
13. **`BatchedLog` tests on a stub `Wal` or stub file.**
    - Three appends in one tick give three pending futures and no sync.
    - Tick N produces exactly one sync, and all three futures complete after it, in order.
    - A lone append waits exactly `maxGroupWaitTicks`.
    - Appends that arrive while a sync is in flight wait for the next sync.
    - A failed sync fails all waiters.
14. **End-to-end test on a real temp file.** Build `SingleThreadedFileIO` inside `TimeBoundedFileIO`, then `Wal`, then `BatchedLog`. Append records, tick until the futures complete, and check each future completes only after the sync. Close, reopen the file with a new `Wal`, call `recover()`, and check the synced records come back in order with the same offsets.
15. **Torn tail test (real file).** Append a half-written record by hand after the last valid one, then recover. Check that recovery stops at the torn record and returns only the valid ones.
16. **Responsibility comments.** A short class comment on `SingleThreadedFileIO`, `TimeBoundedFileIO`, `Wal` and `BatchedLog` saying what the class owns, what it leaves to the layer above, and what it relies on from the layer below. The `Wal` comment includes the atomic-sector assumption.

## Done when
- All the tests above pass, together with the existing 49.
- The end-to-end test reads like a description of the layers.
- Nothing above `SingleThreadedFileIO` knows whether the file is real or simulated.

## After the slice: FDB-style file I/O on the same contract

The slice runs on the tick-driven stack. These tasks bring `FDBStyleFileIO` onto the same `FileIO` contract, so `Wal` and the tests run on either and the two time models can be compared under unchanged upper layers.

17. **Complete `FDBStyleFileIO` and implement `FileIO`.** Add `sync`, `size`, `truncate` and `close` as submitted calls. Give `read` and `write` the full contract: copy the write buffer at call time, loop until all bytes are written, read until the length or EOF. Remove `throws IOException` from the methods, and queue `close()` behind earlier calls. Take a `FileChannel` in the constructor, like `SingleThreadedFileIO`. Keep the built-in timeout as a task scheduled on the queue.
18. **`TaskQueue.advanceTo(long now)`.** One method that does `setNow(now)`, then `drainOtherThreadMessages()`, then `runDue()`, in that order, so submitted tasks are stamped with the new time. It lives on `TaskQueue` because the queue is shared by the whole loop. The signature takes the time because in the FDB model time is an input, unlike `tick()`.
19. **Contract-test fixture.** `FileFixture` with `FileIO fileIO()` and `void driveOnce()`: `fileIO.tick()` for the tick-driven files, `taskQueue.advanceTo(clock)` for `FDBStyleFileIO`. The FDB-style fixture has a real-clock mode and a virtual-time mode where the test chooses `now`.
20. **Parameterize the contract tests.** Move the `SingleThreadedFileIOTest` contract cases into an abstract test over `FileFixture`, and run them on `SingleThreadedFileIO` and `FDBStyleFileIO` (and later `SimulatedFileIO`). Timeout tests stay per implementation, because the mechanisms differ.
21. **Rename the test helper.** `pump` in `FDBStyleFileIOTest` becomes `driveUntilDone(fixture, futures...)`, which loops on `driveOnce()` with a bound so a bug fails instead of hanging. The boolean `realClock` flag goes away; the fixture chooses the clock.
22. **Run the end-to-end test on both stacks.** The same `Wal` test with `SingleThreadedFileIO` inside `TimeBoundedFileIO`, and with `FDBStyleFileIO`.
23. **Second `BatchedLog` variant.** The group wait as a task scheduled on the `TaskQueue` (the FDB commit-proxy style), next to the tick-counted variant. Both share the waiter queue and the completion logic, and differ only in how the flush is triggered.

## File system: creating and opening files

The slice so far takes one open `FileIO`. A process needs more than that: it creates, opens, lists, renames and deletes files, and a restarted process must find its files again. `FileSystem` is the layer that owns names; `FileIO` stays the layer that owns the bytes of one file.

```
FileSystem                 open, exists, list, delete, rename, syncDirectory
   RealFileSystem          a directory on disk; opens a FileChannel and wraps it in SingleThreadedFileIO
   SimulatedFileSystem     in-memory files; owns the crash rule
FileIO                     one open file (unchanged)
```

Decisions:
- Flat namespace: names only, no subdirectories and no permissions.
- Every call returns a future, like `FileIO`, so the simulator can delay or fail it. The driving model is the same: `FileSystem` is the contract, and the tick-driven implementations also have `tick()`.
- A file belongs to the filesystem. The filesystem hands out `FileIO` handles and can invalidate all of them at once, which is how a simulated crash works.
- Directory durability follows POSIX: a created, deleted or renamed file is durable only after `syncDirectory()` completes. A `sync` on the file does not cover its name.
- Same caller rule as `FileIO`: ops execute and complete in the order issued.

24. **`FileSystem` interface and contract.** `open(name, mode)` with `CREATE_NEW`, `OPEN_EXISTING` and `OPEN_OR_CREATE`, returning a `FileIO`; `exists`, `list`, `delete`, `rename`, `syncDirectory`. Write the contract comment: ordering, the directory-durability rule, what `open` fails with (missing file, file already exists), and that handles die with the filesystem. `TickableFileSystem extends FileSystem` adds `tick()`.
25. **`RealFileSystem`.** A directory plus one I/O thread. `open` creates the `FileChannel` and returns a `SingleThreadedFileIO` (inside `TimeBoundedFileIO` if configured). `syncDirectory` fsyncs the directory channel.
26. **Filesystem contract tests.** An abstract test over a `FileSystemFixture` (`driveOnce()` as in task 19): create then open; `CREATE_NEW` on an existing file fails; `OPEN_EXISTING` on a missing file fails; `list` shows created files and not deleted ones; `rename` replaces the name; `delete` of a missing file fails. Run on `RealFileSystem` first.
27. **Use it in the end-to-end test.** Task 14 and the torn-tail test (15) open their file through `RealFileSystem`, and "reopen with a new `Wal`" becomes `fileSystem.open(name, OPEN_EXISTING)`.
28. **`SimulatedFileSystem`.** In-memory files with seeded latency per call, behind `TickableFileSystem`. It returns `SimulatedFileIO` handles (the simulated file, built after the slice). It keeps two states per file: what reads see, and what is durable.
29. **Crash and restart.** `crash()` invalidates every open handle, drops names that were not made durable by `syncDirectory`, and for each file keeps the synced bytes and keeps, drops or tears (at a sector boundary, chosen by the seeded `Random`) the unsynced writes. `restart()` lets the process open its files again. Tests: synced data survives; a created file without `syncDirectory` can vanish; an unsynced tail can be torn; the same seed gives the same outcome. Then run the `Wal` recovery test (15) against it.
30. **Run the filesystem contract tests on `SimulatedFileSystem`**, and on an FDB-style variant if `FDBStyleFileIO` needs one.

## FDB style versus tick style

`FileIO` and the test fixture show where the two styles match and where they differ. The contract is the same, and `driveOnce()` in each fixture holds the whole difference.

| | FDB style (`FDBStyleFileIO`) | Tick style (`SingleThreadedFileIO`, `SimulatedFileIO`) |
|---|---|---|
| Contract | `FileIO`: ordering, durability, copy on write, short reads | the same `FileIO` |
| Who completes futures | the loop thread, from tasks on the queue | the loop thread, inside `tick()` |
| What time is | an input, with the driver choosing `now` | an implicit unit, one tick |
| Driving call | `taskQueue.advanceTo(now)` | `fileIO.tick()` |
| Scheduling | a priority queue ordered by `(time, seq)` | FIFO per file, with latency in ticks |
| Timeout | a task scheduled on the queue | counted in ticks, in a wrapper |
| In a simulation | time jumps to the next event | time moves one tick at a time |
| `BatchedLog` wait | a scheduled task | a tick count |

Because both sit behind `FileIO`, `Wal`, `BatchedLog` and the consensus code run unchanged on either model. A failing test shows which model it came from, because only the fixture differs.

Caveat for the tick column: TigerBeetle has no storage I/O timeout (its io_uring loop says "enforcing timeout here is impossible, so don't even try"). `TimeBoundedFileIO` is our addition, and it is closer to FDB's `ioTimeout` in `AsyncFileKAIO` than to anything in TigerBeetle, only counted in ticks. The tick model is TigerBeetle's. The timeout wrapper is not.

## Not in this slice
Fault injection beyond the crash rule, sector padding, file header, preallocation, rotation, and the port to tickloom. The FDB-style tasks above (17 to 23) come after the slice. See `NOTES.md` for what remains.
