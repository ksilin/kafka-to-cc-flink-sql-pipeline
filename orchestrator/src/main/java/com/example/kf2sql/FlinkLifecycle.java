package com.example.kf2sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps the {@code confluent flink statement} CLI for the operations the orchestrator
 * needs: {@code create}, {@code stop}, {@code describe}, plus a polling helper
 * {@code waitForRunning}.
 *
 * <p>CLI quirks (validated empirically in Phase 05 verification):
 * <ul>
 *   <li>{@code create} uses {@code --environment}; does NOT accept {@code --cloud}/{@code --region}.</li>
 *   <li>{@code list}/{@code stop}/{@code delete}/{@code describe} REQUIRE {@code --cloud}/{@code --region}.</li>
 *   <li>{@code create} with {@code sql.tables.initial-offset-from} property MUST NOT use {@code --wait} —
 *       v2 stays PENDING until v1 is stopped, so blocking would always time out.</li>
 *   <li>For carry-over to work, the referenced statement must be {@code stop}ped, NOT {@code delete}d.</li>
 * </ul>
 */
public class FlinkLifecycle {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ProcessRunner runner;
    private final Config config;

    public FlinkLifecycle(ProcessRunner runner, Config config) {
        this.runner = runner;
        this.config = config;
    }

    /**
     * Submit a new Flink statement.
     *
     * @param name              statement name (must be unique within compute pool)
     * @param sql               full SQL text
     * @param carryOverFromName name of a previous statement whose committed offsets v2
     *                          should inherit. {@code null} for non-carry-over creates.
     */
    public void submit(String name, String sql, String carryOverFromName)
        throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of(
            "confluent", "flink", "statement", "create", name,
            "--sql", sql,
            "--compute-pool", config.computePool,
            "--database", config.cluster,
            "--environment", config.environment));
        if (carryOverFromName == null) {
            cmd.add("--wait");
        } else {
            cmd.add("--property");
            cmd.add("sql.tables.initial-offset-from=" + carryOverFromName);
            // Deliberately NO --wait: v2 enters PENDING and waits for v1 to stop.
        }
        ProcessRunner.Result r = runner.run(cmd);
        if (r.exitCode() != 0) {
            throw new IllegalStateException(
                "submit " + name + " failed: exit=" + r.exitCode()
                + " stderr=" + r.stderr() + " stdout=" + r.stdout());
        }
    }

    /** Stop (NOT delete) a statement. Required for carry-over. */
    public void stop(String name) throws IOException, InterruptedException {
        List<String> cmd = List.of(
            "confluent", "flink", "statement", "stop", name,
            "--cloud", config.cloud,
            "--region", config.region);
        runner.run(cmd);
    }

    /** Return the current status phase (e.g. {@code RUNNING}, {@code PENDING}, {@code FAILED}). */
    public String describe(String name) throws IOException, InterruptedException {
        List<String> cmd = List.of(
            "confluent", "flink", "statement", "describe", name,
            "--cloud", config.cloud,
            "--region", config.region,
            "--output", "json");
        ProcessRunner.Result r = runner.run(cmd);
        if (r.exitCode() != 0) {
            throw new IllegalStateException("describe " + name + " failed: " + r.stderr());
        }
        JsonNode root = MAPPER.readTree(r.stdout());
        JsonNode status = root.get("status");
        if (status == null) {
            return "UNKNOWN";
        }
        // CC's describe schema returns status as a plain string ("COMPLETED", "RUNNING", ...).
        // Older versions / some endpoints nested it as {"phase": "..."}. Handle both.
        if (status.isTextual()) {
            return status.asText();
        }
        JsonNode phase = status.get("phase");
        return phase == null ? "UNKNOWN" : phase.asText();
    }

    /**
     * Poll {@link #describe(String)} until the statement reaches {@code RUNNING}, or throw
     * if it reaches {@code FAILED}, or if {@code timeoutMs} elapses.
     *
     * @param pollIntervalMs sleep between polls; pass 0 in tests to skip sleeping.
     */
    public void waitForRunning(String name, long timeoutMs, long pollIntervalMs)
        throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            String phase = describe(name);
            if ("RUNNING".equals(phase)) {
                return;
            }
            if ("FAILED".equals(phase)) {
                throw new IllegalStateException(
                    "Statement " + name + " entered FAILED state");
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                    "Statement " + name + " did not reach RUNNING within "
                    + timeoutMs + "ms; last phase=" + phase);
            }
            if (pollIntervalMs > 0) {
                Thread.sleep(pollIntervalMs);
            }
        }
    }

    public record Config(String computePool, String cluster, String environment,
                          String cloud, String region) {}
}
