package simfio.fs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The bytes of one simulated file, in two states: what reads see ({@code visible}) and what survives a crash
 * ({@code durable}). A write or truncate changes only {@code visible} and is remembered as unsynced. A sync makes
 * the durable image equal to the visible one. A crash keeps the durable image and, for each unsynced change, may
 * keep it, drop it, or tear a write at a sector boundary. A single sector is written atomically.
 */
final class SimulatedFile {
    static final int SECTOR = 512;

    private record Change(long offset, byte[] data, long truncateTo) {
        boolean isTruncate() {
            return data == null;
        }
    }

    private byte[] visible = new byte[0];
    private byte[] durable = new byte[0];
    private final List<Change> unsynced = new ArrayList<>();

    long size() {
        return visible.length;
    }

    byte[] read(long offset, int length) {
        if (offset >= visible.length) {
            return new byte[0];
        }
        int from = (int) offset;
        return Arrays.copyOfRange(visible, from, Math.min(visible.length, from + length));
    }

    void write(long offset, byte[] data) {
        visible = applyWrite(visible, offset, data);
        unsynced.add(new Change(offset, data, -1));
    }

    void truncate(long newSize) {
        if (newSize >= visible.length) {
            return;
        }
        visible = applyTruncate(visible, newSize);
        unsynced.add(new Change(-1, null, newSize));
    }

    void sync() {
        durable = visible.clone();
        unsynced.clear();
    }

    int unsyncedChanges() {
        return unsynced.size();
    }

    /** Resolves the unsynced changes with the seeded Random, then the file is what a restarted process finds. */
    void crash(Random random) {
        byte[] result = durable;
        for (Change change : unsynced) {
            int outcome = random.nextInt(3);   // 0 dropped, 1 kept, 2 torn
            if (outcome == 0) {
                continue;
            }
            if (change.isTruncate()) {
                if (outcome == 1) {
                    result = applyTruncate(result, change.truncateTo());
                }
                continue;   // a truncate is atomic: kept or dropped
            }
            int length = outcome == 1 ? change.data().length : tornLength(random, change);
            if (length > 0) {
                result = applyWrite(result, change.offset(), Arrays.copyOf(change.data(), length));
            }
        }
        visible = result;
        durable = result.clone();
        unsynced.clear();
    }

    // How much of a write reaches the disk when it tears: up to a random sector boundary inside it. A write that
    // does not cross a boundary is one sector and is atomic, so it is either all there or not there.
    private static int tornLength(Random random, Change change) {
        long start = change.offset();
        long end = start + change.data().length;
        long firstBoundary = (start / SECTOR + 1) * SECTOR;
        if (firstBoundary >= end) {
            return random.nextBoolean() ? change.data().length : 0;
        }
        long boundaries = (end - 1 - firstBoundary) / SECTOR + 1;
        long boundary = firstBoundary + random.nextInt((int) boundaries) * (long) SECTOR;
        return (int) (boundary - start);
    }

    private static byte[] applyWrite(byte[] image, long offset, byte[] data) {
        long end = offset + data.length;
        byte[] result = end > image.length ? Arrays.copyOf(image, (int) end) : image.clone();
        System.arraycopy(data, 0, result, (int) offset, data.length);
        return result;
    }

    private static byte[] applyTruncate(byte[] image, long newSize) {
        return newSize >= image.length ? image : Arrays.copyOf(image, (int) newSize);
    }
}
