package simfio.fs;

import org.junit.jupiter.api.Test;
import simfio.future.FileIO;
import simfio.future.TickCompletableFuture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SimulatedCrashTest {
    private SimulatedFileSystem fs;

    private SimulatedFileSystem newFs(long seed) {
        fs = new SimulatedFileSystem(new Random(seed), 1, 3);
        return fs;
    }

    private <T> T await(TickCompletableFuture<T> future) {
        for (int i = 0; i < 1_000 && future.isPending(); i++) fs.tick();
        assertFalse(future.isPending(), "still pending");
        return future.getResult();
    }

    private static byte[] filled(int length, int value) {
        byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private byte[] contents(FileIO file) {
        long size = await(file.size());
        ByteBuffer read = await(file.read(0, (int) size));
        byte[] bytes = new byte[read.remaining()];
        read.get(bytes);
        return bytes;
    }

    @Test
    void syncedDataAndNamesSurviveEveryCrash() {
        for (long seed = 0; seed < 100; seed++) {
            newFs(seed);
            FileIO file = await(fs.open("wal", OpenMode.CREATE_NEW));
            await(file.write(ByteBuffer.wrap(filled(1000, 7)), 0));
            await(file.sync());
            await(fs.syncDirectory());
            await(file.write(ByteBuffer.wrap(filled(1000, 9)), 1000));   // never synced

            fs.crash();
            fs.restart();

            FileIO reopened = await(fs.open("wal", OpenMode.OPEN_EXISTING));
            byte[] bytes = contents(reopened);
            assertTrue(bytes.length >= 1000, "synced bytes survive, seed " + seed);
            for (int i = 0; i < 1000; i++) assertEquals(7, bytes[i], "seed " + seed);
        }
    }

    @Test
    void aCreatedFileWithoutSyncDirectoryCanVanishAndCanSurvive() {
        Set<Boolean> outcomes = new HashSet<>();
        for (long seed = 0; seed < 50; seed++) {
            newFs(seed);
            await(fs.open("wal", OpenMode.CREATE_NEW));
            fs.crash();
            fs.restart();
            outcomes.add(await(fs.exists("wal")));
        }
        assertEquals(Set.of(true, false), outcomes, "both outcomes must be reachable");
    }

    @Test
    void aDeletedNameCanComeBackWithoutSyncDirectory() {
        Set<Boolean> outcomes = new HashSet<>();
        for (long seed = 0; seed < 50; seed++) {
            newFs(seed);
            await(fs.open("wal", OpenMode.CREATE_NEW));
            await(fs.syncDirectory());
            await(fs.delete("wal"));
            fs.crash();
            fs.restart();
            outcomes.add(await(fs.exists("wal")));
        }
        assertEquals(Set.of(true, false), outcomes);
    }

    @Test
    void anUnsyncedTailCanBeDroppedKeptOrTornAtASectorBoundary() {
        Set<Integer> lengths = new HashSet<>();
        for (long seed = 0; seed < 200; seed++) {
            newFs(seed);
            FileIO file = await(fs.open("wal", OpenMode.CREATE_NEW));
            await(fs.syncDirectory());
            await(file.write(ByteBuffer.wrap(filled(2048, 5)), 0));   // four sectors, never synced
            fs.crash();
            fs.restart();
            byte[] bytes = contents(await(fs.open("wal", OpenMode.OPEN_EXISTING)));
            assertEquals(0, bytes.length % SimulatedFile.SECTOR, "a tear happens on a sector boundary, seed " + seed);
            for (byte b : bytes) assertEquals(5, b);
            lengths.add(bytes.length);
        }
        assertTrue(lengths.contains(0), "dropped");
        assertTrue(lengths.contains(2048), "kept");
        assertTrue(lengths.stream().anyMatch(l -> l > 0 && l < 2048), "torn");
    }

    @Test
    void oldHandlesAreDeadAfterACrash() {
        newFs(1);
        FileIO file = await(fs.open("wal", OpenMode.CREATE_NEW));
        fs.crash();
        TickCompletableFuture<Long> size = file.size();
        assertTrue(size.isFailed());
        assertInstanceOf(IOException.class, size.getException());
        assertTrue(fs.open("wal", OpenMode.OPEN_OR_CREATE).isFailed(), "calls fail until restart()");
    }

    @Test
    void theSameSeedGivesTheSameCrash() {
        assertArrayEquals(crashOutcome(11), crashOutcome(11));
    }

    @Test
    void differentSeedsGiveDifferentCrashes() {
        Set<List<Integer>> outcomes = new HashSet<>();
        for (long seed = 0; seed < 30; seed++) {
            byte[] bytes = crashOutcome(seed);
            outcomes.add(List.of(bytes.length));
        }
        assertTrue(outcomes.size() > 1);
    }

    private byte[] crashOutcome(long seed) {
        newFs(seed);
        FileIO file = await(fs.open("wal", OpenMode.CREATE_NEW));
        await(fs.syncDirectory());
        for (int i = 0; i < 4; i++) await(file.write(ByteBuffer.wrap(filled(700, i + 1)), i * 700L));
        fs.crash();
        fs.restart();
        return contents(await(fs.open("wal", OpenMode.OPEN_EXISTING)));
    }
}
