package simfio.future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileIOTest {

    @TempDir
    Path tempDir;
    @Test
    void write() throws IOException {
        Path filePath = tempDir.resolve("test.txt");
        FileIO.TaskQueue taskQueue = new FileIO.TaskQueue();
        FileIO fileIO = new FileIO(filePath, taskQueue);
        TickCompletableFuture<Integer> writeFuture = fileIO.write(ByteBuffer.wrap("test string".getBytes()), 0);
        while(writeFuture.isPending()) {
            taskQueue.drainOtherThreadMessages();
            taskQueue.runUntilEmpty();
        }

        File file = filePath.toFile();
        FileReader reader = new FileReader(file);
        assertEquals("test string", reader.readAllAsString());

        TickCompletableFuture<ByteBuffer> readFuture = fileIO.read(0, writeFuture.getResult());
        while(readFuture.isPending()) {
            taskQueue.drainOtherThreadMessages();
            taskQueue.runUntilEmpty();
        }

        assertEquals("test string", new String(readFuture.getResult().array()));

    }

    @Test
    void aFailedOperationFailsTheFutureInsteadOfHangingTheCaller() throws IOException {
        FileIO.TaskQueue taskQueue = new FileIO.TaskQueue();
        FileIO fileIO = new FileIO(tempDir.resolve("closed.txt"), taskQueue);
        fileIO.close();

        TickCompletableFuture<ByteBuffer> readFuture = fileIO.read(0, 4);
        TickCompletableFuture<Integer> writeFuture = fileIO.write(ByteBuffer.wrap(new byte[]{1}), 0);

        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while ((readFuture.isPending() || writeFuture.isPending()) && System.nanoTime() < deadline) {
            taskQueue.drainOtherThreadMessages();
            taskQueue.runUntilEmpty();
        }

        assertTrue(readFuture.isFailed());
        assertInstanceOf(java.nio.channels.ClosedChannelException.class, readFuture.getException());
        assertTrue(writeFuture.isFailed());
        assertInstanceOf(java.nio.channels.ClosedChannelException.class, writeFuture.getException());
    }
}
