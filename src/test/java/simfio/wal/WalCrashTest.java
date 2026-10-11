package simfio.wal;

import org.junit.jupiter.api.Test;
import simfio.fs.OpenMode;
import simfio.fs.SimulatedFileSystem;
import simfio.future.FileIO;
import simfio.future.TickCompletableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The whole stack on the simulated disk with a crash at a random moment: SimulatedFileSystem, Wal, BatchedLog.
 * The rule under test is the one a consensus replica relies on: a record whose append future completed is durable,
 * so recovery returns it; the log recovered is always a prefix of what was appended, never a hole or a reordering.
 */
class WalCrashTest {
    private static final int GROUP_WAIT = 3;

    private record Run(int appended, int acknowledged, int recovered) {}

    private SimulatedFileSystem fs;

    private <T> T await(TickCompletableFuture<T> future) {
        for (int i = 0; i < 10_000 && future.isPending(); i++) fs.tick();
        assertFalse(future.isPending(), "still pending");
        return future.getResult();
    }

    private Run crashAndRecover(long seed) {
        Random workload = new Random(seed * 31 + 7);
        fs = new SimulatedFileSystem(new Random(seed), 1, 4);
        FileIO file = await(fs.open("wal", OpenMode.CREATE_NEW));
        await(fs.syncDirectory());
        Wal wal = new Wal(file, 4096);
        await(wal.recover());
        BatchedLog log = new BatchedLog(wal, GROUP_WAIT);

        List<byte[]> appended = new ArrayList<>();
        List<TickCompletableFuture<Long>> futures = new ArrayList<>();
        int rounds = 1 + workload.nextInt(8);
        for (int round = 0; round < rounds; round++) {
            int batch = 1 + workload.nextInt(4);
            for (int i = 0; i < batch; i++) {
                byte[] payload = new byte[1 + workload.nextInt(700)];   // some records cross a 512-byte sector
                workload.nextBytes(payload);
                appended.add(payload);
                futures.add(log.append(payload));
            }
            int ticks = workload.nextInt(12);   // the crash can land mid-wait, mid-write or mid-sync
            for (int t = 0; t < ticks; t++) {
                fs.tick();
                log.tick();
            }
        }
        List<Long> acknowledgedOffsets = new ArrayList<>();
        for (TickCompletableFuture<Long> future : futures) {
            if (future.isCompleted()) acknowledgedOffsets.add(future.getResult());
        }
        int acknowledged = acknowledgedOffsets.size();

        fs.crash();
        fs.restart();

        Wal recoveredWal = new Wal(await(fs.open("wal", OpenMode.OPEN_EXISTING)), 4096);
        List<Wal.Entry> entries = await(recoveredWal.recover());

        // Never a hole or a reordering: the recovered log is a prefix of what was appended.
        assertTrue(entries.size() <= appended.size(), "seed " + seed);
        for (int i = 0; i < entries.size(); i++) {
            assertArrayEquals(appended.get(i), entries.get(i).payload(), "record " + i + ", seed " + seed);
        }
        // Everything acknowledged is there, at the offset the caller was given.
        assertTrue(entries.size() >= acknowledged,
                "acknowledged " + acknowledged + " but recovered " + entries.size() + ", seed " + seed);
        for (int i = 0; i < acknowledged; i++) {
            assertEquals(acknowledgedOffsets.get(i), entries.get(i).offset(), "seed " + seed);
        }
        return new Run(appended.size(), acknowledged, entries.size());
    }

    @Test
    void everyAcknowledgedRecordSurvivesACrashAtAnyMoment() {
        int lostUnacknowledged = 0;
        int keptUnacknowledged = 0;
        int crashedWithNothingAcknowledged = 0;
        for (long seed = 0; seed < 500; seed++) {
            Run run = crashAndRecover(seed);
            if (run.recovered() < run.appended()) lostUnacknowledged++;
            if (run.recovered() > run.acknowledged()) keptUnacknowledged++;
            if (run.acknowledged() == 0) crashedWithNothingAcknowledged++;
        }
        // Non-vacuity: the crash really did lose records, did keep unacknowledged ones, and hit both early and late.
        assertTrue(lostUnacknowledged > 0, "no run lost a record, so the test checks nothing");
        assertTrue(keptUnacknowledged > 0, "no run kept an unacknowledged record");
        assertTrue(crashedWithNothingAcknowledged > 0, "no run crashed before the first acknowledgement");
    }

    @Test
    void theSameSeedGivesTheSameRun() {
        assertEquals(crashAndRecover(123), crashAndRecover(123));
    }

    @Test
    void recoveryCutsOffSurvivorsBeyondAHoleSoTheyCannotComeBack() {
        int hit = 0;
        for (long seed = 0; seed < 300; seed++) {
            fs = new SimulatedFileSystem(new Random(seed), 1, 4);
            FileIO file = await(fs.open("wal", OpenMode.CREATE_NEW));
            await(fs.syncDirectory());
            Wal wal = new Wal(file, 4096);
            await(wal.recover());
            // Three records of the same size, never synced. The crash may keep the third and lose the second.
            for (int i = 1; i <= 3; i++) wal.append(List.of(new byte[100]));
            for (int t = 0; t < 10; t++) fs.tick();
            fs.crash();
            fs.restart();

            Wal first = new Wal(await(fs.open("wal", OpenMode.OPEN_EXISTING)), 4096);
            int survivors = await(first.recover()).size();
            if (survivors < 3) {
                hit++;
            }
            // A new record of the same size lands exactly where the lost one was.
            await(first.append(List.of(new byte[100])));
            await(first.sync());

            Wal second = new Wal(await(fs.open("wal", OpenMode.OPEN_EXISTING)), 4096);
            assertEquals(survivors + 1, await(second.recover()).size(), "an old record came back, seed " + seed);
        }
        assertTrue(hit > 0);
    }
}
