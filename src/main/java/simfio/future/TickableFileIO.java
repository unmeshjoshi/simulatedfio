package simfio.future;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class TickableFileIO {
    private final FileChannel channel;
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    long tick = 0;
    Map<Long, CallDetails> pendingCalls = new HashMap<>();

    public void tick() {
        tick++;
        while (!otherThreadSubmissions.isEmpty()) {
            Runnable poll = otherThreadSubmissions.poll();
            poll.run();
        }

    }

    static record Timeout(long deadline) {
        public boolean isExpired(long now) {
            return now > deadline;
        }
    }

    static class CallDetails {
        Timeout timeout;
        TickCompletableFuture<Integer> future;

        public CallDetails(long deadline, TickCompletableFuture<Integer> future) {
            this.timeout = new Timeout(deadline);
            this.future = future;
        }
    }

    public TickableFileIO(FileChannel channel) {
        this.channel = channel;
    }

    long nextCorrelationId = 0;

    public TickCompletableFuture<Integer> write(ByteBuffer buffer, long offset) throws IOException {
        TickCompletableFuture<Integer> future = new TickCompletableFuture<>();
        long deadline = System.nanoTime() + 10000;
        long correlationId = nextCorrelationId;
        pendingCalls.put(correlationId, new CallDetails(deadline, future));
        executorService.submit(() -> {
            int bytesWritten = 0;
            try {
                bytesWritten = channel.write(buffer, offset);
                handleCallResponse(correlationId, bytesWritten);
            } catch (IOException e) {
                future.fail(e);
            }

        });
        return future;
    }

    LinkedBlockingQueue<Runnable> otherThreadSubmissions = new LinkedBlockingQueue<>();

    private void handleCallResponse(long correlationId, int bytesWritten) {
        try {
            otherThreadSubmissions.put(() -> {
                CallDetails details = pendingCalls.remove(correlationId);
                if (details != null) {
                    details.future.complete(bytesWritten);
                }
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
