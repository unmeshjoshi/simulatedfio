package simfio.future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class FDBStyleFileIOTest {

    @TempDir
    Path tempDir;

    private static long realNowMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    /**
     * The real driver: the clock is the machine's clock. Set it before issuing calls too, because a call schedules
     * its timeout relative to the queue's current time. Bounded so a bug fails instead of hanging.
     */
    private void runUntilDone(FDBStyleFileIO.TaskQueue taskQueue, TickCompletableFuture<?>... futures) {
        runUntilDone(taskQueue, true, futures);
    }

    /** Same loop, but the clock stays where the test put it: virtual time. */
    private void runUntilDoneAtFixedTime(FDBStyleFileIO.TaskQueue taskQueue, TickCompletableFuture<?>... futures) {
        runUntilDone(taskQueue, false, futures);
    }

    private void runUntilDone(FDBStyleFileIO.TaskQueue taskQueue, boolean realClock, TickCompletableFuture<?>... futures) {
        long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < giveUp) {
            boolean pending = false;
            for (TickCompletableFuture<?> f : futures) {
                pending |= f.isPending();
            }
            if (!pending) {
                return;
            }
            if (realClock) {
                taskQueue.setNow(realNowMillis());
            }
            taskQueue.drainOtherThreadMessages();
            taskQueue.runDue();
        }
        fail("futures still pending after 5 seconds");
    }

    @Test
    void write() throws IOException {
        Path filePath = tempDir.resolve("test.txt");
        FDBStyleFileIO.TaskQueue taskQueue = new FDBStyleFileIO.TaskQueue();
        taskQueue.setNow(realNowMillis());
        FDBStyleFileIO fileIO = new FDBStyleFileIO(filePath, taskQueue);
        TickCompletableFuture<Integer> writeFuture = fileIO.write(ByteBuffer.wrap("test string".getBytes()), 0);
        runUntilDone(taskQueue, writeFuture);

        assertEquals("test string", Files.readString(filePath));

        TickCompletableFuture<ByteBuffer> readFuture = fileIO.read(0, writeFuture.getResult());
        runUntilDone(taskQueue, readFuture);

        assertEquals("test string", new String(readFuture.getResult().array()));
    }

    @Test
    void aFailedOperationFailsTheFutureInsteadOfHangingTheCaller() throws IOException {
        FDBStyleFileIO.TaskQueue taskQueue = new FDBStyleFileIO.TaskQueue();
        taskQueue.setNow(realNowMillis());
        FDBStyleFileIO fileIO = new FDBStyleFileIO(tempDir.resolve("closed.txt"), taskQueue);
        fileIO.close();

        TickCompletableFuture<ByteBuffer> readFuture = fileIO.read(0, 4);
        TickCompletableFuture<Integer> writeFuture = fileIO.write(ByteBuffer.wrap(new byte[]{1}), 0);
        runUntilDone(taskQueue, readFuture, writeFuture);

        assertTrue(readFuture.isFailed());
        assertInstanceOf(ClosedChannelException.class, readFuture.getException());
        assertTrue(writeFuture.isFailed());
        assertInstanceOf(ClosedChannelException.class, writeFuture.getException());
    }

    @Test
    void aCallTimesOutWhenTheClockReachesItsDeadline() throws Exception {
        FDBStyleFileIO.TaskQueue taskQueue = new FDBStyleFileIO.TaskQueue();
        FDBStyleFileIO fileIO = new FDBStyleFileIO(tempDir.resolve("timeout.txt"), taskQueue);
        // Hold the single I/O thread so the write cannot start until we release it.
        CountDownLatch release = new CountDownLatch(1);
        fileIO.executorService.submit(() -> {
            try {
                release.await();
            } catch (InterruptedException ignored) {
            }
        });
        TickCompletableFuture<Integer> write = fileIO.write(ByteBuffer.wrap("late".getBytes()), 0);

        // Virtual time: no sleeping, we just move the clock.
        taskQueue.setNow(FDBStyleFileIO.TIMEOUT_MS - 1);
        taskQueue.runDue();
        assertTrue(write.isPending(), "must not time out before its deadline");

        taskQueue.setNow(FDBStyleFileIO.TIMEOUT_MS);
        taskQueue.runDue();
        assertTrue(write.isFailed());
        assertInstanceOf(TimeoutException.class, write.getException());

        // The write still runs once the I/O thread is free. Its completion finds the future done and is dropped.
        release.countDown();
        TickCompletableFuture<ByteBuffer> read = fileIO.read(0, 4);
        runUntilDoneAtFixedTime(taskQueue, read);

        assertTrue(write.isFailed());
        assertEquals("late", new String(read.getResult().array()));
    }

    @Test
    void tasksDueAtTheSameTimeRunInTheOrderTheyWereScheduled() {
        FDBStyleFileIO.TaskQueue taskQueue = new FDBStyleFileIO.TaskQueue();
        StringBuilder order = new StringBuilder();
        taskQueue.schedule(5, () -> order.append("a"));
        taskQueue.schedule(5, () -> order.append("b"));
        taskQueue.schedule(2, () -> order.append("c"));
        taskQueue.schedule(5, () -> order.append("d"));

        taskQueue.setNow(5);
        taskQueue.runDue();

        assertEquals("cabd", order.toString());
    }

    @Test
    void tasksNotYetDueDoNotRun() {
        FDBStyleFileIO.TaskQueue taskQueue = new FDBStyleFileIO.TaskQueue();
        StringBuilder order = new StringBuilder();
        taskQueue.schedule(10, () -> order.append("late"));

        taskQueue.setNow(9);
        taskQueue.runDue();
        assertEquals("", order.toString());

        taskQueue.setNow(10);
        taskQueue.runDue();
        assertEquals("late", order.toString());
    }
}
