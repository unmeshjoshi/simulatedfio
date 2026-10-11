package simfio.fs;

import simfio.future.TickCompletableFuture;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * The tick-style engine of the simulated disk: a FIFO of operations, each due some ticks after it was submitted.
 * The latency is drawn from the seeded Random, and an operation is never due before an earlier one, so calls
 * complete in the order they were issued. A call takes effect at the moment it completes, inside {@link #tick()}.
 */
final class OpQueue {
    private record Op(long dueTick, Runnable complete, Runnable effectOnly) {}

    private final Random random;
    private final int minLatencyTicks;
    private final int maxLatencyTicks;
    private final Deque<Op> queue = new ArrayDeque<>();
    private long ticks;
    private long lastDueTick;

    OpQueue(Random random, int minLatencyTicks, int maxLatencyTicks) {
        if (minLatencyTicks < 1 || maxLatencyTicks < minLatencyTicks) {
            throw new IllegalArgumentException("need 1 <= minLatencyTicks <= maxLatencyTicks");
        }
        this.random = random;
        this.minLatencyTicks = minLatencyTicks;
        this.maxLatencyTicks = maxLatencyTicks;
    }

    /**
     * Queues an action. It runs when it falls due and its result completes the returned future.
     * If {@code mutates}, a crash while it is still queued may apply its effect without completing the future.
     */
    <T> TickCompletableFuture<T> submit(Callable<T> action, boolean mutates) {
        TickCompletableFuture<T> future = new TickCompletableFuture<>();
        Runnable complete = () -> {
            T result;
            try {
                result = action.call();
            } catch (Throwable e) {
                future.fail(e);
                return;
            }
            future.complete(result);
        };
        Runnable effectOnly = mutates ? () -> {
            try {
                action.call();
            } catch (Throwable ignored) {
                // the effect failed, so there is nothing to apply
            }
        } : null;
        long latency = minLatencyTicks + random.nextInt(maxLatencyTicks - minLatencyTicks + 1);
        long due = Math.max(lastDueTick, ticks + latency);
        lastDueTick = due;
        queue.add(new Op(due, complete, effectOnly));
        return future;
    }

    /** Advances one tick and completes every call that is now due, in order. */
    void tick() {
        ticks++;
        while (!queue.isEmpty() && queue.peekFirst().dueTick() <= ticks) {
            queue.pollFirst().complete().run();
        }
    }

    /** A crash: the queued calls never complete, but the ones that change data may have reached the disk first. */
    void crash() {
        for (Op op : queue) {
            if (op.effectOnly() != null) {
                op.effectOnly().run();
            }
        }
        queue.clear();
    }

    boolean isIdle() {
        return queue.isEmpty();
    }
}
