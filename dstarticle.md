# DST article: review notes and open work

Notes on the Deterministic Simulation Testing article, kept so the work can be picked up later.

- Article: `~/work/rebecca-mfcom/articles/deterministic-simulation-testing/deterministic-simulation-testing.xml`
- Build: `rake build/articles/deterministic-simulation-testing.html` (run from `~/work/rebecca-mfcom`)
- Diagrams: `articles/deterministic-simulation-testing/diagrams/*.puml` (19 diagrams plus `style.puml`), rendered to `img/*.svg`
  - `rake dst:diagrams` re-renders stale SVGs; `rake dst:diagrams_all` re-renders all of them
  - Uses `PLANTUML_JAR` if set (e.g. `~/work/MADSPlantUMLSteps/plantuml-1.2024.8.jar`), otherwise the `plantuml` command
- Status: nothing is committed yet in rebecca-mfcom. The XML, 19 SVGs and `rake.rb` are modified, and `diagrams/` is untracked. Commit messages use the form `deterministic-simulation-testing: ...`.

## Pinned references

- Tickloom commit `31a8ab4f18042eb6328907fec38ea5735b6fa2a3`. It is on origin/main, and all links to it were checked and return 200.
- FoundationDB commit `3a238d638ae146737b784884dba7052e8d40397e` (apple/foundationdb)
- TigerBeetle commit `97c7a8ef385270ebe0e1b75959d3d21d134629df`. `src/vsr/cluster/network.zig` returned 503 when checked, so it is not linked.

## What has been done

- Replaced all code snippets with prose and GitHub links pinned to the tickloom commit. The article has 0 `<pre>` blocks.
- Fixed facts that had gone stale:
  - The driver loop runs `advanceTimeForAllProcesses(1)`, then one network tick, then clients, then servers. A Node ticks `messageBus`, then `process`, then `fileSystem`.
  - `SimulatedStorage` is now a `SimulatedFileSystem` for each process.
  - `assertEventually` is now `cluster.tickUntil` / `tickUntilComplete`.
  - Added the `onStart()` lifecycle and the early return when a process is STOPPED.
- Added sections:
  - The Simulated File System
  - A Write-Ahead Log on the Simulated Disk
  - Where to Cut: Messages or Bytes
- Added a paragraph to the Driver Loop section comparing TigerBeetle's per-component ticks with FoundationDB's shared virtual-time task queue.
- Added WalBackedKVStore to the Quorum section, along with the rule that a replica replies only after its write is durable.
- Moved the diagrams' PlantUML sources into the repo and added the rake tasks.

## Rating (overall 6/10)

| Dimension | Score | Why |
|---|---|---|
| Technical accuracy | 8/10 | Every claim was checked against tickloom at the pinned commit, and the TigerBeetle/FDB comparisons against their source. A few small errors remain (see nits). |
| Depth and coverage | 7/10 | Covers the network, disk, time and execution. The material on where to cut the simulation is a strength. Missing: limitations, liveness, clock skew, and how to check that a run is deterministic. |
| Evidence | 4/10 | The biggest weakness. Reproducing a bug from its seed is never shown, the running example's history is linearizable, and the speed claims are not measured. |
| Structure and flow | 5/10 | After the problem setup it becomes a class-by-class tour. The four things it says must become controllable are never revisited one by one. Comparisons with other systems are scattered across three sections. The payoff comes late. |
| Prose | 6/10 | The second half is clear. The first half (roughly lines 1–211, which came from slides) still reads like slides. "Deterministic simulation testing" is not used until the fifth section. |
| Figures | 7/10 | Plenty of figures and consistent, but none for storage, and one contradicts the text. |
| Scholarship and conventions | 3/10 | The bibliography is empty and there are no acknowledgments. |

Estimated score after fixes:
1. Worked bug example: about 7.5
2. Restructure around clock, execution, network and disk, plus a limitations section: about 8.5
3. Prose pass on the first half, a measured speed figure and a bibliography: 9 or more, which is publishable

## Blocking issues

1. **No worked bug example.** Show a seed, the faults it injected, the history that isn't linearizable (checked by Jepsen/Knossos), and the same seed reproducing it.
   - `FaultInjectingSimulationRunnerTest` with seed 123 at 500 ticks gives a *linearizable* history (`:valid? true`). The test only uses `assertDoesNotThrow`.
   - Candidates for a failing example:
     - `DDIA_LinearizabilityScenarioTest`
     - `ConsistencyPropertiesTest`
     - `NonlinearizableQuorumReadScenarioTest`, which is cited in the `llm-and-dsls` article
   - Commit `7040657` fixed a durability bug: a skipped set was acknowledged before the newer write was durable. Its test, `QuorumReplicaConcurrentSetTest`, drives one replica by hand rather than a seeded cluster. If a seeded cluster run could find this bug, it would be a good real-world story.
2. **The speed claims are not backed up** ("years into minutes"). Measure them: two 500-tick runs take about 0.29 s. Report ticks per second, or simulated time per second of real time.
3. **No limitations section.** It should cover:
   - liveness is not checked (TigerBeetle has `transition_to_liveness_mode`)
   - no clock skew (TigerBeetle's `testing/time.zig` has linear, periodic, step and non_ideal offsets)
   - a fixed fault rate (FDB has BUGGIFY plus 261 randomized knobs in `ServerKnobs.cpp`; TigerBeetle has `options_swarm`)
   - messages are never duplicated
   - no random crashes in the fault runner, which injects network faults only
   - code outside the simulated layer is not tested, such as the `NioConnection` framing
   - the cost of retrofitting an existing system
4. **The bibliography is empty and there are no acknowledgments.** Cite:
   - FoundationDB's simulation talk/paper
   - TigerBeetle's VOPR and liveness posts
   - Jepsen/Knossos
   - the `llm-and-dsls` article

## Major issues

5. **Structure.** Consider organizing the article around the four sources of nondeterminism (clock, execution, network, disk) instead of tickloom's classes. Gather the TigerBeetle and FDB comparisons into each of those sections, or into one section. This needs the author's decision.
6. **The first half needs rewriting** as prose in the author's voice, and should introduce the term "deterministic simulation testing" early. This needs the author's decision.

## Nits

7. "A failed sync is fatal" appears in the list of POSIX rules, but it is a policy (PostgreSQL's fsyncgate response), not a POSIX rule.
8. One list says "process execution" and another says "thread scheduling" for the same thing. Use one name.
9. Figures:
   - `tick_future_network_sequence.svg` shows a blocking `get()`, which contradicts the text.
   - The architecture figure has no FileSystem.

## Polish

10. Each of the two `ClusterCrashRecoveryTest` links should point at its own test method (`#L…`).
11. Some text is repeated, mostly captions that restate the paragraph.
12. Heading case is inconsistent.
13. `quorum_test_integration_focus.svg` is never used. Use it or delete it.
14. Check the version date (2026-05-15).
15. Add storage figures: visible vs durable copies, a crash as a power cut, and the torn WAL tail.

## Additions that are still pending (not drafted yet)

- **Response vs timeout tie.** The network ticks before the nodes, so a response that arrives on the same tick as a timeout wins.
- **Callbacks.** Work started from a callback completes on the next tick.
- **Determinism check.** Tickloom has `SimulationRunnerTest.historyWithSameSeedShouldBeIdenticalAcrossRuns` and `FaultInjectingSimulationRunnerTest.sameSeedShouldProduceSameFaultScheduleAndHistory`. FDB reruns about 5% of tests to check determinism ("unseed"; `contrib/TestHarness2/test_harness/run.py`, `unseed_check_ratio=0.05`).
- **Clock skew.** Explain it, and that tickloom doesn't simulate it.
- **Limitations paragraph.** See blocking item 3.

## Verified facts to reuse

- `FaultInjectingSimulationRunner`:
  - `FAULT_PROBABILITY_PER_TICK=0.08`, `HEAL_EVERY_TICKS=40`, `MAX_DELAY_TICKS=8`
  - four fault types, each logged to `faultSchedule`
  - network faults only
- `ServerMain.runEventLoop`:
  - runs `network.runToNanos(MAX_IDLE_MS=10ms)`, then `replica.tick()`, then `fileSystem.tick()`, so a production tick is about 10 ms or more
  - uses `RealFileSystem`
- Where the network is simulated:
  - Tickloom and TigerBeetle simulate at the message level. TigerBeetle uses `packet_simulator.zig` with `PacketSimulatorType(*Message)`.
  - FoundationDB simulates at the byte-stream level: `Sim2Conn` in `sim2.actor.cpp`, with the real `FlowTransport` framing running on top.
  - `bytestream-network.md` in this folder has a design for byte-stream simulation in tickloom (not implemented).
- Where the disk is simulated:
  - Tickloom simulates at the level of files.
  - FDB uses `AsyncFileNonDurable`.
  - TigerBeetle simulates the block device in `storage.zig`, with `ClusterFaultAtlas`, read/write faults, sector corruption and misdirected writes.
- `Wal` is the mechanism: CRC32C, and it truncates at the torn tail. `BatchedLog` is the group-commit policy.
- `ClusterCrashRecoveryTest` runs 50 seeds.

## Out-of-date items in tickloom (reported, not fixed)

- The `Cluster.crashNode` javadoc still says "Its in-memory `Storage` is not affected".
- TODO.md items S1–S4 are done but still unchecked.
- TODO.md item P1 is stale, because production ticks are already about 10 ms or more.
