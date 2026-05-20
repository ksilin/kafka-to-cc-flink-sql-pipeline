package com.example.kf2sql.quarkus;

import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test against real Confluent Cloud. No mocks.
 *
 * <p>NOT a @QuarkusTest — we instantiate FlinkLifecycle directly with real config
 * and real ProcessBuilder calls. This mirrors the plain-Java variant's
 * FlinkLifecycleCcIT approach.
 *
 * <p>Run with: {@code ./gradlew test -Dcc.integration=true}
 * Skip by default via the {@code cc.integration} system property check in
 * {@link #checkEnabled()}.
 *
 * <p>Prerequisites: logged-in {@code confluent} CLI with access to env-nvv5xz.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlinkLifecycleCcIT {

    private static FlinkLifecycle lifecycle;
    private static String testTopic;
    private static String testStatementName;

    @BeforeAll
    static void setup() {
        checkEnabled();

        lifecycle = new FlinkLifecycle();
        // Inject config values directly (no CDI in non-Quarkus test)
        setField(lifecycle, "computePool", System.getProperty("cc.compute-pool", "lfcp-kknvdm"));
        setField(lifecycle, "cluster", System.getProperty("cc.cluster", "lkc-6w3rv2"));
        setField(lifecycle, "environment", System.getProperty("cc.environment", "env-nvv5xz"));
        setField(lifecycle, "cloud", System.getProperty("cc.cloud", "aws"));
        setField(lifecycle, "region", System.getProperty("cc.region", "eu-central-1"));

        long suffix = System.currentTimeMillis();
        testTopic = "kf-qit-" + suffix;
        testStatementName = "kf-qit-ddl-" + suffix;
    }

    @AfterAll
    static void cleanup() {
        if (lifecycle == null) return;
        try {
            lifecycle.stop(testStatementName).await().atMost(Duration.ofSeconds(30));
        } catch (Exception ignored) {}
        try {
            // delete via ProcessBuilder directly (FlinkLifecycle doesn't expose delete)
            new ProcessBuilder("confluent", "flink", "statement", "delete", testStatementName,
                    "--cloud", "aws", "--region", "eu-central-1", "--force")
                    .start().waitFor();
        } catch (Exception ignored) {}
        try {
            new ProcessBuilder("confluent", "kafka", "topic", "delete", testTopic,
                    "--cluster", System.getProperty("cc.cluster", "lkc-6w3rv2"), "--force")
                    .start().waitFor();
        } catch (Exception ignored) {}
        try {
            new ProcessBuilder("confluent", "schema-registry", "subject", "delete",
                    testTopic + "-value", "--force").start().waitFor();
            new ProcessBuilder("confluent", "schema-registry", "subject", "delete",
                    testTopic + "-value", "--permanent", "--force").start().waitFor();
        } catch (Exception ignored) {}
    }

    @Test
    @Order(1)
    void submit_ddl_completesWithoutError() {
        String sql = "CREATE TABLE `" + testTopic + "` ("
                + "id BIGINT, name STRING"
                + ") WITH ("
                + "'value.format' = 'json-registry',"
                + "'changelog.mode' = 'append'"
                + ");";

        assertDoesNotThrow(() ->
                lifecycle.submit(testStatementName, sql, null)
                        .await().atMost(Duration.ofSeconds(120)));
    }

    @Test
    @Order(2)
    void describe_returnsCompletedPhase() {
        String phase = lifecycle.describe(testStatementName)
                .await().atMost(Duration.ofSeconds(30));
        assertEquals("COMPLETED", phase,
                "DDL statement should be COMPLETED after submit returned");
    }

    @Test
    @Order(3)
    void waitForRunning_throwsOnCompletedStatement() {
        assertThrows(Exception.class, () ->
                lifecycle.waitForRunning(testStatementName, Duration.ofSeconds(5))
                        .await().atMost(Duration.ofSeconds(10)),
                "COMPLETED is not RUNNING — should timeout");
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static void checkEnabled() {
        if (!"true".equals(System.getProperty("cc.integration"))) {
            throw new org.opentest4j.TestAbortedException(
                    "CC integration tests skipped. Run with -Dcc.integration=true");
        }
    }

    private static void setField(Object obj, String fieldName, String value) {
        try {
            var field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + fieldName, e);
        }
    }
}
