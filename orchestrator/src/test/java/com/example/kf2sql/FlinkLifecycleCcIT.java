package com.example.kf2sql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 08.1 — CC integration test for {@link FlinkLifecycle}.
 *
 * <p>Validates that {@link FlinkLifecycle} + {@link RealProcessRunner} translate to CLI calls
 * that real Confluent Cloud accepts. Does NOT replicate Phase 05's data-flow validation —
 * just exercises the wrappers end-to-end on a minimal CREATE TABLE + describe + delete cycle.
 *
 * <p>Tagged {@code cc-integration} — skipped by {@code mvn test}; runs only with
 * {@code mvn verify -DccIntegration=true}.
 *
 * <p>Configuration via system properties (or defaults below):
 * <ul>
 *   <li>{@code cc.environment} (default {@code env-nvv5xz})</li>
 *   <li>{@code cc.cluster}     (default {@code lkc-6w3rv2})</li>
 *   <li>{@code cc.computePool} (default {@code lfcp-kknvdm})</li>
 *   <li>{@code cc.cloud}       (default {@code aws})</li>
 *   <li>{@code cc.region}      (default {@code eu-central-1})</li>
 * </ul>
 *
 * <p>Pre-flight: caller must have a logged-in {@code confluent} CLI with access to the
 * environment, plus the relevant Kafka API key for the cluster (see
 * {@code docs/cc-flink-lessons-agent.md}).
 *
 * <p>The test creates a uniquely-named statement and topic per run (suffix = current time
 * millis) so concurrent or repeated runs don't collide.
 */
@Tag("cc-integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlinkLifecycleCcIT {

    private static FlinkLifecycle lifecycle;
    private static String testTopic;
    private static String testStatementName;

    @BeforeAll
    static void setUpAll() {
        lifecycle = new FlinkLifecycle(
            new RealProcessRunner(),
            new FlinkLifecycle.Config(
                System.getProperty("cc.computePool", "lfcp-kknvdm"),
                System.getProperty("cc.cluster",     "lkc-6w3rv2"),
                System.getProperty("cc.environment", "env-nvv5xz"),
                System.getProperty("cc.cloud",       "aws"),
                System.getProperty("cc.region",      "eu-central-1")));
        long suffix = System.currentTimeMillis();
        testTopic = "kf-it-" + suffix;
        testStatementName = "kf-it-ddl-" + suffix;
    }

    @AfterAll
    static void cleanUp() throws IOException, InterruptedException {
        // Best-effort: remove anything we created
        try {
            new RealProcessRunner().run(List.of(
                "confluent", "flink", "statement", "delete", testStatementName,
                "--cloud", "aws", "--region", "eu-central-1", "--force"));
        } catch (Exception ignored) {}
        try {
            new RealProcessRunner().run(List.of(
                "confluent", "kafka", "topic", "delete", testTopic,
                "--cluster", System.getProperty("cc.cluster", "lkc-6w3rv2"), "--force"));
        } catch (Exception ignored) {}
        try {
            new RealProcessRunner().run(List.of(
                "confluent", "schema-registry", "subject", "delete",
                testTopic + "-value", "--force"));
            new RealProcessRunner().run(List.of(
                "confluent", "schema-registry", "subject", "delete",
                testTopic + "-value", "--permanent", "--force"));
        } catch (Exception ignored) {}
    }

    @AfterEach
    void afterEach() {
        // No-op; @AfterAll handles cleanup so the test order matters.
    }

    @Test
    @Order(1)
    void submit_ddl_reachesCompleted() throws IOException, InterruptedException {
        String sql = "CREATE TABLE `" + testTopic + "` ("
            + "  id BIGINT,"
            + "  name STRING"
            + ") WITH ("
            + "  'value.format' = 'json-registry',"
            + "  'changelog.mode' = 'append'"
            + ");";

        // submit() with no carry-over uses --wait, which blocks until COMPLETED for DDL.
        lifecycle.submit(testStatementName, sql, /* carryOverFromName= */ null);
    }

    @Test
    @Order(2)
    void describe_returnsCompletedPhase() throws IOException, InterruptedException {
        String phase = lifecycle.describe(testStatementName);
        assertEquals("COMPLETED", phase,
            "DDL statement should be in COMPLETED phase right after submit() returned");
    }

    @Test
    @Order(3)
    void waitForRunning_throwsOnCompletedNotRunning() throws IOException, InterruptedException {
        // A COMPLETED DDL is NOT in RUNNING; waitForRunning should throw on timeout.
        // (The wrapper currently only short-circuits on RUNNING/FAILED — COMPLETED hits the deadline.)
        assertThrows(IllegalStateException.class,
            () -> lifecycle.waitForRunning(testStatementName, 2000, 500),
            "Polling a COMPLETED statement for RUNNING should time out");
    }
}
