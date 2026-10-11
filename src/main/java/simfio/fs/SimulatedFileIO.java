package simfio.fs;

import simfio.future.TickCompletableFuture;
import simfio.future.TickableFileIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * A file in memory, on the tick model. Every call completes some ticks later (seeded latency, at least one), in
 * the order issued, and takes effect when it completes. The same {@code FileIO} contract as the real file.
 *
 * What it adds over the real one: {@link SimulatedFile} keeps what is durable apart from what is visible, so a
 * crash (see {@link SimulatedFileSystem#crash()}) can drop or tear writes that were never synced.
 *
 * After the file is closed, or after a crash, new calls fail with an IOException.
 */
public class SimulatedFileIO implements TickableFileIO {
    private final SimulatedFile file;
    private final OpQueue queue;
    private boolean closed;
    private boolean crashed;

    SimulatedFileIO(SimulatedFile file, Random random, int minLatencyTicks, int maxLatencyTicks) {
        this.file = file;
        this.queue = new OpQueue(random, minLatencyTicks, maxLatencyTicks);
    }

    /** A standalone file with no filesystem around it. */
    public static SimulatedFileIO standalone(Random random, int minLatencyTicks, int maxLatencyTicks) {
        return new SimulatedFileIO(new SimulatedFile(), random, minLatencyTicks, maxLatencyTicks);
    }

    @Override
    public void tick() {
        queue.tick();
    }

    @Override
    public TickCompletableFuture<ByteBuffer> read(long offset, int length) {
        return submit(() -> ByteBuffer.wrap(file.read(offset, length)), false);
    }

    @Override
    public TickCompletableFuture<Integer> write(ByteBuffer buffer, long offset) {
        // Copy now: the write takes effect later, and the caller may reuse its buffer.
        byte[] data = new byte[buffer.remaining()];
        buffer.duplicate().get(data);
        return submit(() -> {
            file.write(offset, data);
            return data.length;
        }, true);
    }

    @Override
    public TickCompletableFuture<Void> sync() {
        return submit(() -> {
            file.sync();
            return null;
        }, false);
    }

    @Override
    public TickCompletableFuture<Long> size() {
        return submit(file::size, false);
    }

    @Override
    public TickCompletableFuture<Void> truncate(long newSize) {
        return submit(() -> {
            file.truncate(newSize);
            return null;
        }, true);
    }

    @Override
    public TickCompletableFuture<Void> close() {
        TickCompletableFuture<Void> result = submit(() -> null, false);
        closed = true;   // queued behind the earlier calls, which still finish; later calls fail
        return result;
    }

    /** The process died: calls still queued never complete, and the handle is dead. */
    void crash() {
        crashed = true;
        queue.crash();
    }

    private <T> TickCompletableFuture<T> submit(java.util.concurrent.Callable<T> action, boolean mutates) {
        if (crashed || closed) {
            TickCompletableFuture<T> rejected = new TickCompletableFuture<>();
            rejected.fail(new IOException(crashed ? "file system crashed" : "file is closed"));
            return rejected;
        }
        return queue.submit(action, mutates);
    }
}
