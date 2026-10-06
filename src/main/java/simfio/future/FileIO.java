package simfio.future;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.PriorityQueue;
import java.util.concurrent.*;

public class FileIO {
    private final TaskQueue taskQueue;
    private final FileChannel channel;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    static record Task(long time, Runnable runnable) implements Comparable<Task> {
        @Override
        public int compareTo(Task o) {
            return Long.compare(time, o.time);
        }
    }

    static class TaskQueue {
        LinkedBlockingQueue<Runnable> otherThreadSubmissions = new LinkedBlockingQueue<>();

        PriorityQueue<Task> queue = new PriorityQueue<>();

        public void runUntilEmpty() throws IOException {
            while (!queue.isEmpty()) {
                Task task = queue.poll();
                task.runnable.run();
            }
        }

        public void drainOtherThreadMessages() {
            while (!otherThreadSubmissions.isEmpty()) {
                Runnable poll = otherThreadSubmissions.poll();
                queue.add(new Task(System.nanoTime(), poll));
            }
        }
    }

    public FileIO(Path filePath, TaskQueue taskQueue) throws IOException {
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
        taskQueue.queue.add(new Task(System.nanoTime() + 10000, () -> {
            future.fail(new TimeoutException("Timed out waiting for task to complete"));
        }));
        try {
            executorService.submit(() -> {
                try {
                    T result = call.run();
                    taskQueue.otherThreadSubmissions.add(() -> {
                        if(!future.isCompleted()) return;
                        future.complete(result);
                    });
                } catch (Throwable e) {
                    taskQueue.otherThreadSubmissions.add(() -> {
                        if(!future.isCompleted()) return;
                        future.fail(e);
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
