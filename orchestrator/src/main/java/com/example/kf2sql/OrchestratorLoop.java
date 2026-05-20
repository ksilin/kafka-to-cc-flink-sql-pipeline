package com.example.kf2sql;

import java.io.IOException;

/**
 * Per-message orchestration: takes a {@link Subscription}, decides subscribe/update/unsubscribe,
 * issues the right CC Flink calls, and returns an {@link AckMessage}.
 *
 * Implements CONTRACT §2 (subscription update lifecycle) for V1 stateless filters.
 *
 * V1 PoC: single-threaded, no Kafka producer/consumer in this class. Caller (Main) is responsible
 * for reading the subscription from a file and printing/publishing the ACK.
 */
public class OrchestratorLoop {

    private final SqlGenerator sqlGenerator;
    private final StatementNameAllocator allocator;
    private final FlinkLifecycle flink;
    private final String inputTopic;
    private final String outputTopic;
    private final long waitForRunningTimeoutMs;
    private final long pollIntervalMs;

    public OrchestratorLoop(
        SqlGenerator sqlGenerator,
        StatementNameAllocator allocator,
        FlinkLifecycle flink,
        String inputTopic,
        String outputTopic,
        long waitForRunningTimeoutMs,
        long pollIntervalMs
    ) {
        this.sqlGenerator = sqlGenerator;
        this.allocator = allocator;
        this.flink = flink;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.waitForRunningTimeoutMs = waitForRunningTimeoutMs;
        this.pollIntervalMs = pollIntervalMs;
    }

    public AckMessage handle(Subscription sub) throws IOException, InterruptedException {
        try {
            if (sub.isUnsubscribe()) {
                return handleUnsubscribe(sub);
            }
            String previous = allocator.current(sub.vehicleId());
            if (previous == null) {
                return handleSubscribe(sub);
            } else {
                return handleUpdate(sub, previous);
            }
        } catch (IllegalStateException ex) {
            return AckMessage.error(sub.correlationId(), ex.getMessage());
        }
    }

    private AckMessage handleSubscribe(Subscription sub) throws IOException, InterruptedException {
        String name = allocator.next(sub.vehicleId());
        String sql = sqlGenerator.fromSubscription(sub, inputTopic, outputTopic);
        flink.submit(name, sql, /* carryOverFromName= */ null);
        return AckMessage.success(sub.correlationId(), "subscribed");
    }

    private AckMessage handleUpdate(Subscription sub, String previousName)
        throws IOException, InterruptedException {
        // 1) submit v2 with carry-over (NO --wait)
        String newName = allocator.next(sub.vehicleId());
        String sql = sqlGenerator.fromSubscription(sub, inputTopic, outputTopic);
        flink.submit(newName, sql, previousName);
        // 2) stop v1 — required for v2 to transition out of PENDING
        flink.stop(previousName);
        // 3) poll v2 until RUNNING
        flink.waitForRunning(newName, waitForRunningTimeoutMs, pollIntervalMs);
        return AckMessage.success(sub.correlationId(), "updated");
    }

    private AckMessage handleUnsubscribe(Subscription sub)
        throws IOException, InterruptedException {
        String previous = allocator.current(sub.vehicleId());
        if (previous != null) {
            flink.stop(previous);
        }
        return AckMessage.success(sub.correlationId(), "unsubscribed");
    }
}
