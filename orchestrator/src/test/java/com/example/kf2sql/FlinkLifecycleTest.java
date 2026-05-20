package com.example.kf2sql;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 07.5 RED tests for {@link FlinkLifecycle}.
 *
 * Uses a stub {@link ProcessRunner} so no real {@code confluent} CLI is invoked.
 * The integration test {@link FlinkLifecycleCcIT} (cc-integration tag) exercises
 * the real CLI; skipped by default (run with {@code mvn verify -DccIntegration=true}).
 */
class FlinkLifecycleTest {

    @Test
    void submit_invokesCorrectCli() throws IOException, InterruptedException {
        StubRunner runner = new StubRunner(new ProcessRunner.Result(0, "OK", ""));
        FlinkLifecycle lifecycle = new FlinkLifecycle(runner, defaultConfig());

        lifecycle.submit("kf-test-stmt", "INSERT INTO foo SELECT * FROM bar;", null);

        assertEquals(1, runner.calls.size());
        List<String> cmd = runner.calls.get(0);
        // Verify the exact verb + key flags
        assertTrue(cmd.containsAll(List.of(
            "confluent", "flink", "statement", "create",
            "kf-test-stmt",
            "--sql", "INSERT INTO foo SELECT * FROM bar;",
            "--compute-pool", "lfcp-test",
            "--database", "lkc-test",
            "--environment", "env-test",
            "--wait")),
            "Expected create flags missing: " + cmd);
        // Carry-over property NOT present when null
        assertFalse(cmd.contains("--property"));
    }

    @Test
    void submit_withCarryOver_addsProperty() throws IOException, InterruptedException {
        StubRunner runner = new StubRunner(new ProcessRunner.Result(0, "OK", ""));
        FlinkLifecycle lifecycle = new FlinkLifecycle(runner, defaultConfig());

        lifecycle.submit("kf-v2", "INSERT INTO foo SELECT 1;", "kf-v1");

        List<String> cmd = runner.calls.get(0);
        assertTrue(cmd.contains("--property"));
        int idx = cmd.indexOf("--property");
        assertEquals("sql.tables.initial-offset-from=kf-v1", cmd.get(idx + 1));
        // Carry-over PENDING is the expected state, so we MUST NOT pass --wait
        assertFalse(cmd.contains("--wait"),
            "Carry-over create must NOT --wait — v2 stays PENDING until v1 stops");
    }

    @Test
    void stop_invokesCorrectCli() throws IOException, InterruptedException {
        StubRunner runner = new StubRunner(new ProcessRunner.Result(0, "OK", ""));
        FlinkLifecycle lifecycle = new FlinkLifecycle(runner, defaultConfig());

        lifecycle.stop("kf-old");

        List<String> cmd = runner.calls.get(0);
        assertTrue(cmd.containsAll(List.of(
            "confluent", "flink", "statement", "stop", "kf-old",
            "--cloud", "aws", "--region", "eu-central-1")),
            "stop must use --cloud/--region (different schema from create): " + cmd);
    }

    @Test
    void describe_parsesStatusFromJson() throws IOException, InterruptedException {
        String describeJson = """
            {"name":"kf-x","status":{"phase":"RUNNING","detail":"Running."}}
            """;
        StubRunner runner = new StubRunner(new ProcessRunner.Result(0, describeJson, ""));
        FlinkLifecycle lifecycle = new FlinkLifecycle(runner, defaultConfig());

        assertEquals("RUNNING", lifecycle.describe("kf-x"));
    }

    @Test
    void waitForRunning_returnsImmediatelyWhenAlreadyRunning() throws IOException, InterruptedException {
        StubRunner runner = new StubRunner(new ProcessRunner.Result(0,
            "{\"status\":{\"phase\":\"RUNNING\"}}", ""));
        FlinkLifecycle lifecycle = new FlinkLifecycle(runner, defaultConfig());

        lifecycle.waitForRunning("kf-x", 1000, 0);
        assertEquals(1, runner.calls.size(), "Should describe once and exit");
    }

    @Test
    void waitForRunning_throwsOnFailed() throws IOException, InterruptedException {
        StubRunner runner = new StubRunner(new ProcessRunner.Result(0,
            "{\"status\":{\"phase\":\"FAILED\",\"detail\":\"SQL parse error\"}}", ""));
        FlinkLifecycle lifecycle = new FlinkLifecycle(runner, defaultConfig());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> lifecycle.waitForRunning("kf-x", 1000, 0));
        assertTrue(ex.getMessage().contains("FAILED"));
    }

    private static FlinkLifecycle.Config defaultConfig() {
        return new FlinkLifecycle.Config(
            "lfcp-test", "lkc-test", "env-test", "aws", "eu-central-1");
    }

    /** Test stub: records every call, returns a pre-canned Result. */
    private static class StubRunner implements ProcessRunner {
        final List<List<String>> calls = new ArrayList<>();
        final Result canned;

        StubRunner(Result canned) {
            this.canned = canned;
        }

        @Override
        public Result run(List<String> command) {
            calls.add(List.copyOf(command));
            return canned;
        }
    }
}
