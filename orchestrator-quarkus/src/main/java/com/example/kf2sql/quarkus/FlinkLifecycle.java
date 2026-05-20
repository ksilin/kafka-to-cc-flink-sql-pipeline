package com.example.kf2sql.quarkus;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps {@code confluent flink statement} CLI. Same logic as the plain-Java
 * {@code FlinkLifecycle}, but uses Mutiny Uni for async composition and
 * Quarkus config injection for CC parameters.
 *
 * Lessons encoded (from cc-flink-lessons):
 * - create uses --environment, NOT --cloud/--region
 * - carry-over create MUST NOT use --wait
 * - stop (NOT delete) is required for carry-over reference
 * - describe returns status as plain string (not nested object)
 */
@ApplicationScoped
public class FlinkLifecycle {

    @ConfigProperty(name = "cc.compute-pool")
    String computePool;

    @ConfigProperty(name = "cc.cluster")
    String cluster;

    @ConfigProperty(name = "cc.environment")
    String environment;

    @ConfigProperty(name = "cc.cloud")
    String cloud;

    @ConfigProperty(name = "cc.region")
    String region;

    public Uni<Void> submit(String name, String sql, String carryOverFromName) {
        return runProcess(() -> {
            List<String> cmd = new ArrayList<>(List.of(
                    "confluent", "flink", "statement", "create", name,
                    "--sql", sql,
                    "--compute-pool", computePool,
                    "--database", cluster,
                    "--environment", environment));
            if (carryOverFromName == null) {
                cmd.add("--wait");
            } else {
                cmd.add("--property");
                cmd.add("sql.tables.initial-offset-from=" + carryOverFromName);
            }
            return cmd;
        }).replaceWithVoid();
    }

    public Uni<Void> stop(String name) {
        return runProcess(() -> List.of(
                "confluent", "flink", "statement", "stop", name,
                "--cloud", cloud, "--region", region
        )).replaceWithVoid();
    }

    public Uni<String> describe(String name) {
        return runProcess(() -> List.of(
                "confluent", "flink", "statement", "describe", name,
                "--cloud", cloud, "--region", region,
                "--output", "json"
        )).map(output -> {
            if (output.contains("\"status\":")) {
                int idx = output.indexOf("\"status\":");
                String after = output.substring(idx + 9).trim();
                if (after.startsWith("\"")) {
                    // Plain string: "status": "RUNNING"
                    int end = after.indexOf("\"", 1);
                    return after.substring(1, end);
                }
                if (after.startsWith("{") && after.contains("\"phase\":")) {
                    // Nested object: "status": {"phase": "COMPLETED"}
                    int phaseIdx = after.indexOf("\"phase\":");
                    String phaseAfter = after.substring(phaseIdx + 8).trim();
                    if (phaseAfter.startsWith("\"")) {
                        int end = phaseAfter.indexOf("\"", 1);
                        return phaseAfter.substring(1, end);
                    }
                }
            }
            return "UNKNOWN";
        });
    }

    public Uni<Void> waitForRunning(String name, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        return describe(name)
                .onItem().transformToUni(phase -> {
                    if ("RUNNING".equals(phase)) return Uni.createFrom().voidItem();
                    if ("FAILED".equals(phase))
                        return Uni.createFrom().<Void>failure(
                                new IllegalStateException("Statement " + name + " FAILED"));
                    if (System.currentTimeMillis() >= deadline)
                        return Uni.createFrom().<Void>failure(
                                new IllegalStateException("Timeout waiting for " + name + " RUNNING"));
                    Log.infof("Polling %s: phase=%s", name, phase);
                    return Uni.createFrom().voidItem()
                            .onItem().delayIt().by(Duration.ofSeconds(5))
                            .chain(() -> waitForRunning(name, Duration.ofMillis(deadline - System.currentTimeMillis())));
                });
    }

    private Uni<String> runProcess(java.util.function.Supplier<List<String>> cmdSupplier) {
        return Uni.createFrom().item(cmdSupplier)
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .map(cmd -> executeProcess(cmd));
    }

    protected String executeProcess(List<String> cmd) {
        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            Process proc = pb.start();
            String stdout = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String stderr = new String(proc.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            int exit = proc.waitFor();
            if (exit != 0) {
                throw new RuntimeException("CLI failed (exit=" + exit + "): " + stderr);
            }
            return stdout;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("CLI invocation failed", e);
        }
    }
}
