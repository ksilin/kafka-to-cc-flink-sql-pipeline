package com.example.kf2sql.quarkus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FlinkLifecycle} CLI command generation.
 * Uses a spy that captures ProcessBuilder commands instead of executing them.
 * Mirrors plain-Java variant's FlinkLifecycleTest (6 tests).
 */
class FlinkLifecycleUnitTest {

    private FlinkLifecycle lifecycle;
    private List<List<String>> capturedCommands;

    @BeforeEach
    void setup() {
        lifecycle = new SpyFlinkLifecycle();
        setField(lifecycle, "computePool", "lfcp-test");
        setField(lifecycle, "cluster", "lkc-test");
        setField(lifecycle, "environment", "env-test");
        setField(lifecycle, "cloud", "aws");
        setField(lifecycle, "region", "eu-central-1");
        capturedCommands = ((SpyFlinkLifecycle) lifecycle).commands;
    }

    @Test
    void submit_withoutCarryOver_usesWait() {
        lifecycle.submit("kf-stmt-1", "INSERT INTO foo SELECT 1;", null)
                .await().atMost(Duration.ofSeconds(5));

        assertEquals(1, capturedCommands.size());
        List<String> cmd = capturedCommands.get(0);
        assertTrue(cmd.contains("create"));
        assertTrue(cmd.contains("kf-stmt-1"));
        assertTrue(cmd.contains("--wait"), "Non-carry-over submit MUST use --wait");
        assertTrue(cmd.contains("--environment"), "create uses --environment");
        assertFalse(cmd.contains("--cloud"), "create must NOT use --cloud");
        assertFalse(cmd.contains("--region"), "create must NOT use --region");
        assertFalse(cmd.contains("--property"), "No property without carry-over");
    }

    @Test
    void submit_withCarryOver_omitsWaitAndAddsProperty() {
        lifecycle.submit("kf-v2", "INSERT INTO foo SELECT 1;", "kf-v1")
                .await().atMost(Duration.ofSeconds(5));

        List<String> cmd = capturedCommands.get(0);
        assertFalse(cmd.contains("--wait"),
                "Carry-over submit MUST NOT use --wait — v2 stays PENDING until v1 stops");
        assertTrue(cmd.contains("--property"));
        int propIdx = cmd.indexOf("--property");
        assertEquals("sql.tables.initial-offset-from=kf-v1", cmd.get(propIdx + 1));
    }

    @Test
    void stop_usesCloudAndRegion() {
        lifecycle.stop("kf-old")
                .await().atMost(Duration.ofSeconds(5));

        List<String> cmd = capturedCommands.get(0);
        assertTrue(cmd.contains("stop"));
        assertTrue(cmd.contains("kf-old"));
        assertTrue(cmd.contains("--cloud"), "stop MUST use --cloud");
        assertTrue(cmd.contains("--region"), "stop MUST use --region");
        assertFalse(cmd.contains("--environment"), "stop must NOT use --environment");
    }

    @Test
    void describe_usesCloudRegionAndJsonOutput() {
        lifecycle.describe("kf-x")
                .await().atMost(Duration.ofSeconds(5));

        List<String> cmd = capturedCommands.get(0);
        assertTrue(cmd.contains("describe"));
        assertTrue(cmd.contains("--cloud"));
        assertTrue(cmd.contains("--region"));
        assertTrue(cmd.contains("--output"));
        assertTrue(cmd.contains("json"));
    }

    @Test
    void describe_parsesPlainStringStatus() {
        ((SpyFlinkLifecycle) lifecycle).stubbedOutput = "{\"status\": \"RUNNING\", \"status_detail\": \"ok\"}";

        String phase = lifecycle.describe("kf-x")
                .await().atMost(Duration.ofSeconds(5));

        assertEquals("RUNNING", phase);
    }

    @Test
    void describe_parsesNestedObjectStatus() {
        ((SpyFlinkLifecycle) lifecycle).stubbedOutput = "{\"status\": {\"phase\": \"COMPLETED\"}}";

        String phase = lifecycle.describe("kf-x")
                .await().atMost(Duration.ofSeconds(5));

        assertEquals("COMPLETED", phase);
    }

    // ─── Spy: captures commands, returns stubbed output ──────────────────────

    static class SpyFlinkLifecycle extends FlinkLifecycle {
        final List<List<String>> commands = new ArrayList<>();
        String stubbedOutput = "{}";

        @Override
        protected String executeProcess(List<String> cmd) {
            commands.add(List.copyOf(cmd));
            return stubbedOutput;
        }
    }

    private static void setField(Object obj, String fieldName, String value) {
        try {
            var field = obj.getClass().getSuperclass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (NoSuchFieldException e) {
            try {
                var field = obj.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(obj, value);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to set field " + fieldName, ex);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + fieldName, e);
        }
    }
}
