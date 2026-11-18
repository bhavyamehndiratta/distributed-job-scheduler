package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Runs one actor (a scheduler node, worker or client) on a single thread, which
 * is what the actors assume. Incoming messages, tick() calls and anything
 * posted from outside all execute on that one thread, in order.
 */
public final class Driver implements AutoCloseable {

    private final String name;
    private final long tickNanos;
    private final LinkedBlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
    private volatile boolean running = true;
    private volatile BiConsumer<NodeId, Message> handler = (from, message) -> { };
    private Thread thread;

    public Driver(String name, long tickMillis) {
        this.name = name;
        this.tickNanos = TimeUnit.MILLISECONDS.toNanos(tickMillis);
    }

    public void setHandler(BiConsumer<NodeId, Message> handler) {
        this.handler = handler;
    }

    private static final int MAX_TASKS_PER_BATCH = 256;
    private volatile Runnable afterBatch = () -> { };

    /** Runs on the driver thread after each batch of tasks and ticks. Used to fsync, then release messages. */
    public void setAfterBatch(Runnable afterBatch) {
        this.afterBatch = afterBatch;
    }

    /** Called from network threads. */
    public void postIncoming(NodeId from, Message message) {
        tasks.offer(() -> handler.accept(from, message));
    }

    public void post(Runnable task) {
        tasks.offer(task);
    }

    /** Runs the supplier on the driver thread and waits for its result. */
    public <T> T call(Supplier<T> supplier, long timeoutMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        post(() -> {
            try {
                future.complete(supplier.get());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted");
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("driver " + name + " did not answer in " + timeoutMillis + " ms");
        }
    }

    public void start(Runnable tick) {
        thread = new Thread(() -> loop(tick), "driver-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    private void loop(Runnable tick) {
        long next = System.nanoTime() + tickNanos;
        while (running) {
            try {
                long wait = next - System.nanoTime();
                Runnable task = wait > 0 ? tasks.poll(wait, TimeUnit.NANOSECONDS) : tasks.poll();
                boolean worked = false;
                int handled = 0;
                while (task != null) {
                    task.run();
                    worked = true;
                    if (++handled >= MAX_TASKS_PER_BATCH) {
                        break;
                    }
                    task = tasks.poll();
                }
                long now = System.nanoTime();
                if (now >= next) {
                    tick.run();
                    worked = true;
                    next += tickNanos;
                    if (now - next > 10 * tickNanos) {
                        next = now + tickNanos; // far behind: skip ahead instead of bursting ticks
                    }
                }
                if (worked) {
                    afterBatch.run();
                }
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
