package com.example.kf2sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 07.6 RED tests for {@link OrchestratorLoop}.
 *
 * Three lifecycle paths covered:
 * <ul>
 *   <li>SUBSCRIBE — first time for a vehicleId. Submit statement, ACK Success/subscribed.</li>
 *   <li>UPDATE — vehicleId already has a statement. Submit v2 with carry-over, stop v1, wait
 *       for v2 RUNNING, ACK Success/updated.</li>
 *   <li>UNSUBSCRIBE — empty dataIdList. Stop current statement (if any), ACK Success/unsubscribed.</li>
 * </ul>
 *
 * The harness uses a fake {@link FlinkLifecycle} backed by a fake {@link ProcessRunner}.
 * No real CC writes.
 */
class OrchestratorLoopTest {

    private static final String TEMPLATE_BODY = """
        INSERT INTO __OUTPUT_TOPIC__
        SELECT * FROM __INPUT_TOPIC__
        WHERE vehicleId = '__VEHICLE_ID__'
          AND mdc_id IN (__MDC_ID_CSV__);
        """;

    @Test
    void subscribe_firstTime_submitsAndAcks(@TempDir Path tmp) throws IOException, InterruptedException {
        var harness = new Harness(tmp);
        Subscription sub = new Subscription("V1", "c1", List.of("100", "200"));

        AckMessage ack = harness.loop.handle(sub);

        assertEquals("Success", ack.status());
        assertEquals("subscribed", ack.details());
        assertEquals("c1", ack.correlationId());

        // Exactly one CLI call: statement create
        assertEquals(1, harness.runner.calls.size(), "Subscribe = 1 create call");
        List<String> cmd = harness.runner.calls.get(0);
        assertTrue(cmd.contains("create"));
        assertTrue(cmd.contains("--wait"), "Initial submit uses --wait (no carry-over)");
    }

    @Test
    void update_secondTime_carryOverAndStopAndAck(@TempDir Path tmp) throws IOException, InterruptedException {
        var harness = new Harness(tmp);
        // First subscribe to seed the allocator
        harness.loop.handle(new Subscription("V1", "c1", List.of("100", "200")));
        harness.runner.calls.clear();
        // Configure the runner to respond RUNNING to describe so waitForRunning exits
        harness.runner.describeResponse = "{\"status\":{\"phase\":\"RUNNING\"}}";

        Subscription sub2 = new Subscription("V1", "c2", List.of("200", "300"));
        AckMessage ack = harness.loop.handle(sub2);

        assertEquals("Success", ack.status());
        assertEquals("updated", ack.details());
        assertEquals("c2", ack.correlationId());

        // Update path: 1 create (v2 carry-over, no --wait) + 1 stop (v1) + 1+ describe (v2 polling)
        long createCount = harness.runner.calls.stream()
            .filter(cmd -> cmd.contains("create")).count();
        long stopCount = harness.runner.calls.stream()
            .filter(cmd -> cmd.contains("stop")).count();
        long describeCount = harness.runner.calls.stream()
            .filter(cmd -> cmd.contains("describe")).count();
        assertEquals(1, createCount, "v2 created once");
        assertEquals(1, stopCount, "v1 stopped once");
        assertTrue(describeCount >= 1, "v2 polled at least once");

        // The create call must NOT have --wait (carry-over), and MUST have the property
        List<String> createCmd = harness.runner.calls.stream()
            .filter(cmd -> cmd.contains("create")).findFirst().orElseThrow();
        assertFalse(createCmd.contains("--wait"));
        assertTrue(createCmd.stream().anyMatch(s -> s.startsWith("sql.tables.initial-offset-from=kf-flt-")));
    }

    @Test
    void unsubscribe_emptyList_stopsAndAcks(@TempDir Path tmp) throws IOException, InterruptedException {
        var harness = new Harness(tmp);
        // Seed: subscribe first
        harness.loop.handle(new Subscription("V1", "c1", List.of("100", "200")));
        harness.runner.calls.clear();

        Subscription unsub = new Subscription("V1", "c2", List.of());
        AckMessage ack = harness.loop.handle(unsub);

        assertEquals("Success", ack.status());
        assertEquals("unsubscribed", ack.details());

        // Unsubscribe path: stop only, no create, no describe.
        assertEquals(1, harness.runner.calls.size());
        assertTrue(harness.runner.calls.get(0).contains("stop"));
    }

    @Test
    void unsubscribe_withoutPriorSubscribe_isIdempotent(@TempDir Path tmp) throws IOException, InterruptedException {
        var harness = new Harness(tmp);
        Subscription unsub = new Subscription("Vnew", "c1", List.of());

        AckMessage ack = harness.loop.handle(unsub);

        assertEquals("Success", ack.status());
        assertEquals("unsubscribed", ack.details());
        // No CLI calls — nothing to stop
        assertEquals(0, harness.runner.calls.size());
    }

    @Test
    void create_failure_propagatesAsErrorAck(@TempDir Path tmp) throws IOException, InterruptedException {
        var harness = new Harness(tmp);
        harness.runner.failNext = true;
        harness.runner.failureMessage = "SQL parse failed at line 5";

        Subscription sub = new Subscription("V1", "c1", List.of("100"));
        AckMessage ack = harness.loop.handle(sub);

        assertEquals("Error", ack.status());
        assertTrue(ack.details().contains("SQL parse failed"),
            "ACK error details must include the CC error message");
    }

    /** Test harness: builds an OrchestratorLoop wired against a stub ProcessRunner. */
    private static class Harness {
        final RecordingRunner runner = new RecordingRunner();
        final OrchestratorLoop loop;

        Harness(Path tmp) throws IOException {
            FlinkLifecycle lifecycle = new FlinkLifecycle(runner,
                new FlinkLifecycle.Config("lfcp-test", "lkc-test", "env-test", "aws", "eu-central-1"));
            StatementNameAllocator allocator = new StatementNameAllocator(tmp.resolve("state.json"));
            SqlGenerator sqlGen = new SqlGenerator(TEMPLATE_BODY);
            loop = new OrchestratorLoop(
                sqlGen, allocator, lifecycle,
                "kf-input-test", "kf-data-test",
                /* waitForRunningTimeoutMs = */ 1000, /* pollIntervalMs = */ 0);
        }
    }

    /** Records every command and returns canned responses. */
    private static class RecordingRunner implements ProcessRunner {
        final List<List<String>> calls = new ArrayList<>();
        boolean failNext = false;
        String failureMessage = "";
        String describeResponse = "{\"status\":{\"phase\":\"RUNNING\"}}";

        @Override
        public Result run(List<String> command) {
            calls.add(List.copyOf(command));
            if (failNext) {
                failNext = false;
                return new Result(1, "", failureMessage);
            }
            if (command.contains("describe")) {
                return new Result(0, describeResponse, "");
            }
            return new Result(0, "OK", "");
        }
    }
}
