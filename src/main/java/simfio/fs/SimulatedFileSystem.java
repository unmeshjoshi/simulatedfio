package simfio.fs;

import simfio.future.FileIO;
import simfio.future.TickCompletableFuture;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * A flat directory in memory, on the tick model, with the crash rule.
 *
 * Names: {@code names} is what calls see, {@code durableNames} is what survives a crash. A create, delete or rename
 * changes only {@code names} and is remembered as unsynced until {@link #syncDirectory()}.
 *
 * {@link #crash()} models a power cut: calls still queued never complete; each unsynced name change is kept or
 * dropped; each surviving file keeps its synced bytes and keeps, drops or tears its unsynced writes. Every choice
 * comes from the seeded Random, so the same seed gives the same crash. {@link #restart()} lets the process open its
 * files again; the old handles stay dead.
 */
public class SimulatedFileSystem implements TickableFileSystem {
    private sealed interface NameChange {
        record Create(String name, SimulatedFile file) implements NameChange {}
        record Delete(String name) implements NameChange {}
        record Rename(String from, String to) implements NameChange {}
    }

    private final Random random;
    private final int minLatencyTicks;
    private final int maxLatencyTicks;
    private final OpQueue queue;
    private final List<SimulatedFileIO> handles = new ArrayList<>();
    private final List<NameChange> unsyncedNames = new ArrayList<>();
    private TreeMap<String, SimulatedFile> names = new TreeMap<>();
    private TreeMap<String, SimulatedFile> durableNames = new TreeMap<>();
    private boolean crashed;

    public SimulatedFileSystem(Random random, int minLatencyTicks, int maxLatencyTicks) {
        this.random = random;
        this.minLatencyTicks = minLatencyTicks;
        this.maxLatencyTicks = maxLatencyTicks;
        this.queue = new OpQueue(random, minLatencyTicks, maxLatencyTicks);
    }

    @Override
    public void tick() {
        queue.tick();
        for (SimulatedFileIO handle : handles) {
            handle.tick();
        }
    }

    @Override
    public TickCompletableFuture<FileIO> open(String name, OpenMode mode) {
        return submit(() -> {
            SimulatedFile file = names.get(name);
            switch (mode) {
                case CREATE_NEW -> {
                    if (file != null) throw new FileAlreadyExistsException(name);
                }
                case OPEN_EXISTING -> {
                    if (file == null) throw new NoSuchFileException(name);
                }
                case OPEN_OR_CREATE -> { }
            }
            if (file == null) {
                file = new SimulatedFile();
                names.put(name, file);
                unsyncedNames.add(new NameChange.Create(name, file));
            }
            SimulatedFileIO handle = new SimulatedFileIO(file, random, minLatencyTicks, maxLatencyTicks);
            handles.add(handle);
            return (FileIO) handle;
        }, true);
    }

    @Override
    public TickCompletableFuture<Boolean> exists(String name) {
        return submit(() -> names.containsKey(name), false);
    }

    @Override
    public TickCompletableFuture<List<String>> list() {
        return submit(() -> List.copyOf(names.keySet()), false);
    }

    @Override
    public TickCompletableFuture<Void> delete(String name) {
        return submit(() -> {
            if (names.remove(name) == null) throw new NoSuchFileException(name);
            unsyncedNames.add(new NameChange.Delete(name));
            return null;
        }, true);
    }

    @Override
    public TickCompletableFuture<Void> rename(String from, String to) {
        return submit(() -> {
            SimulatedFile file = names.remove(from);
            if (file == null) throw new NoSuchFileException(from);
            names.put(to, file);
            unsyncedNames.add(new NameChange.Rename(from, to));
            return null;
        }, true);
    }

    @Override
    public TickCompletableFuture<Void> syncDirectory() {
        return submit(() -> {
            durableNames = new TreeMap<>(names);
            unsyncedNames.clear();
            return null;
        }, false);
    }

    /** True when no name operation is queued. File handles have their own queues. */
    public boolean isIdle() {
        return queue.isIdle();
    }

    /**
     * Power cut. Open handles die, queued calls never complete (the ones that change data may have taken effect),
     * and the disk is left as the crash rule allows.
     */
    public void crash() {
        queue.crash();   // may add to unsyncedNames and handles: a queued create or rename may have reached the disk
        for (SimulatedFileIO handle : handles) {
            handle.crash();
        }
        handles.clear();

        TreeMap<String, SimulatedFile> survivors = new TreeMap<>(durableNames);
        for (NameChange change : unsyncedNames) {
            if (random.nextBoolean()) {
                applyChange(survivors, change);
            }
        }
        names = survivors;
        durableNames = new TreeMap<>(survivors);
        unsyncedNames.clear();

        Map<SimulatedFile, Boolean> seen = new IdentityHashMap<>();
        for (SimulatedFile file : survivors.values()) {   // sorted by name, so the order is deterministic
            if (seen.put(file, true) == null) {
                file.crash(random);
            }
        }
        crashed = true;
    }

    /** The process comes back. It must open its files again. */
    public void restart() {
        crashed = false;
    }

    private static void applyChange(Map<String, SimulatedFile> target, NameChange change) {
        switch (change) {
            case NameChange.Create create -> target.put(create.name(), create.file());
            case NameChange.Delete delete -> target.remove(delete.name());
            case NameChange.Rename rename -> {
                SimulatedFile file = target.remove(rename.from());
                if (file != null) {
                    target.put(rename.to(), file);
                }
            }
        }
    }

    private <T> TickCompletableFuture<T> submit(Callable<T> action, boolean mutates) {
        if (crashed) {
            TickCompletableFuture<T> rejected = new TickCompletableFuture<>();
            rejected.fail(new IOException("file system crashed"));
            return rejected;
        }
        return queue.submit(action, mutates);
    }
}
