package simfio.future;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.PriorityQueue;
import java.util.concurrent.*;

public class FDBStyleFileIO {
    // Time is in milliseconds on the task queue's clock, which the driver sets.
    static final long TIMEOUT_MS = 1_000;

    private final TaskQueue taskQueue;
    private final FileChannel channel;
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /** Ordered by time, then by the order they were scheduled, so tasks due at the same time run first come first. */
    static record Task(long time, long seq, Runnable runnable) implements Comparable<Task> {
        @Override
        public int compareTo(Task o) {
            int byTime = Long.compare(time, o.time);
            return byTime != 0 ? byTime : Long.compare(seq, o.seq);
        }
    }

    static class TaskQueue {
        // The only structure other threads may touch.
        final LinkedBlockingQueue<Runnable> otherThreadSubmissions = new LinkedBlockingQueue<>();

        final PriorityQueue<Task> queue = new PriorityQueue<>();
        private long now = 0;
        private long nextSeq = 0;

        /** The driver owns the clock: real time in production, the next task's time in a simulation. */
        public void setNow(long now) {
            assert now >= this.now : "time must not go backwards";
            this.now = now;
        }

        public void schedule(long delay, Runnable runnable) {
            queue.add(new Task(now + delay, nextSeq++, runnable));
        }

        /** Moves tasks submitted from other threads onto the queue, stamped with the current time. */
        public void drainOtherThreadMessages() {
            Runnable task;
            while ((task = otherThreadSubmissions.poll()) != null) {
                schedule(0, task);
            }
        }

        /** Runs every task that is due at the current time, including ones that tasks schedule with no delay. */
        public void runDue() {
            while (!queue.isEmpty() && queue.peek().time() <= now) {
                queue.poll().runnable().run();
            }
        }
    }

    public FDBStyleFileIO(Path filePath, TaskQueue taskQueue) throws IOException {
        this.taskQueue = taskQueue;
        //this needs to be moved outside so that it can be async.
        this.channel = FileChannel.open(filePath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
    }

    public TickCompletableFuture<ByteBuffer> read(long offset, int length) throws IOException {
        return submit(() -> {
            ByteBuffer allocate = ByteBuffer.allocate(length);
            channel.read(allocate, offset);
            return allocate;
        });
    }

    public TickCompletableFuture<Integer> write(ByteBuffer buffer, long offset) throws IOException {
        return submit(() -> channel.write(buffer, offset));
    }

    private interface IoCall<T> {
        T run() throws Exception;
    }

    /**
     * Runs the blocking call on the I/O thread. Whatever happens, the outcome reaches the future: the I/O thread never
     * completes it, it posts a task that completes (or fails) it on the task queue's thread. Without this, an
     * exception thrown inside the executor would be held in a Future nobody reads and the caller would wait forever.
     */
    private <T> TickCompletableFuture<T> submit(IoCall<T> call) {
        TickCompletableFuture<T> future = new TickCompletableFuture<>();
        // The timeout is just another task on the queue. If the call finishes first, it finds the future done and does nothing.
        taskQueue.schedule(TIMEOUT_MS, () -> {
            if (future.isPending()) {
                future.fail(new TimeoutException("Timed out waiting for task to complete"));
            }
        });
        try {
            executorService.submit(() -> {
                try {
                    T result = call.run();
                    taskQueue.otherThreadSubmissions.add(() -> {
                        if (future.isPending()) { // not if it already timed out
                            future.complete(result);
                        }
                    });
                } catch (Throwable e) {
                    taskQueue.otherThreadSubmissions.add(() -> {
                        if (future.isPending()) {
                            future.fail(e);
                        }
                    });
                }
            });
        } catch (RejectedExecutionException e) {
            future.fail(e); // the executor is shut down; nothing was queued
        }
        return future;
    }

    public void close() throws IOException {
        channel.close();
    }

}
