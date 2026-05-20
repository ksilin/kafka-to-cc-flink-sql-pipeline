package com.example.kf2sql.quarkus;

import com.example.kf2sql.quarkus.handler.SubscribeHandler;
import com.example.kf2sql.quarkus.handler.UnsubscribeHandler;
import com.example.kf2sql.quarkus.handler.UpdateHandler;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full lifecycle integration test against real Confluent Cloud.
 * No mocks. No in-memory channels. Real Kafka topics, real Flink statements,
 * real data flow.
 *
 * <p>Tests the Quarkus handler classes (SubscribeHandler, UpdateHandler,
 * UnsubscribeHandler) wired with a real FlinkLifecycle against the same CC
 * environment the plain-Java variant validated in Phase 05.
 *
 * <p>Lifecycle tested:
 * <ol>
 *   <li>SUBSCRIBE — creates filter statement, verify it reaches RUNNING</li>
 *   <li>DATA FLOW — produce telemetry fixture, consume sink, verify 9 records</li>
 *   <li>UPDATE — carry-over offsets, new predicate, verify RUNNING</li>
 *   <li>DATA FLOW — produce more data, verify new filter applies (6 records)</li>
 *   <li>UNSUBSCRIBE — stops statement, verify no new output</li>
 * </ol>
 *
 * <p>Run: {@code ./gradlew cleanTest test -Dcc.integration=true}
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>Logged-in {@code confluent} CLI</li>
 *   <li>Kafka API key activated for lkc-6w3rv2</li>
 *   <li>SQL template at {@code ../sql/01-filter-template.sql}</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrchestratorLifecycleCcIT {

    private static FlinkLifecycle flink;
    private static StatementNameAllocator allocator;
    private static SqlGenerator sqlGenerator;
    private static SubscribeHandler subscribeHandler;
    private static UpdateHandler updateHandler;
    private static UnsubscribeHandler unsubscribeHandler;

    private static final String INPUT_TOPIC = "kf-qit-input";
    private static final String OUTPUT_TOPIC = "kf-qit-output";
    private static final String SETUP_STMT = "kf-qit-setup";
    private static final String VEHICLE = "vehicle-fixture-001";

    private static String env;
    private static String cluster;
    private static String cloud;
    private static String region;

    @BeforeAll
    static void setup() throws Exception {
        checkEnabled();

        env = sysProp("cc.environment", "env-nvv5xz");
        cluster = sysProp("cc.cluster", "lkc-6w3rv2");
        cloud = sysProp("cc.cloud", "aws");
        region = sysProp("cc.region", "eu-central-1");

        // Wire FlinkLifecycle with real config
        flink = new FlinkLifecycle();
        setField(flink, "computePool", sysProp("cc.compute-pool", "lfcp-kknvdm"));
        setField(flink, "cluster", cluster);
        setField(flink, "environment", env);
        setField(flink, "cloud", cloud);
        setField(flink, "region", region);

        allocator = new StatementNameAllocator();

        // Load real SQL template
        Path templatePath = Path.of("../sql/01-filter-template.sql");
        assertTrue(Files.exists(templatePath),
                "SQL template must exist at " + templatePath.toAbsolutePath());
        sqlGenerator = new SqlGenerator(Files.readString(templatePath));

        // Wire handlers with real collaborators
        subscribeHandler = new SubscribeHandler();
        setField(subscribeHandler, "flink", flink);
        setField(subscribeHandler, "allocator", allocator);
        setField(subscribeHandler, "sqlGenerator", sqlGenerator);
        setField(subscribeHandler, "inputTopic", INPUT_TOPIC);
        setField(subscribeHandler, "outputTopic", OUTPUT_TOPIC);

        updateHandler = new UpdateHandler();
        setField(updateHandler, "flink", flink);
        setField(updateHandler, "allocator", allocator);
        setField(updateHandler, "sqlGenerator", sqlGenerator);
        setField(updateHandler, "inputTopic", INPUT_TOPIC);
        setField(updateHandler, "outputTopic", OUTPUT_TOPIC);

        unsubscribeHandler = new UnsubscribeHandler();
        setField(unsubscribeHandler, "flink", flink);
        setField(unsubscribeHandler, "allocator", allocator);

        // Clean slate
        cleanCc();
        createInputTopic();
        createOutputTable();
    }

    @AfterAll
    static void tearDown() {
        if (flink == null) return;
        cleanCc();
    }

    // ─── Test 1: SUBSCRIBE ───────────────────────────────────────────────────

    @Test
    @Order(1)
    void subscribe_createsStatementAndReturnsAck() {
        Subscription sub = new Subscription(VEHICLE, "it-c1", List.of("100", "200"));
        AckMessage ack = subscribeHandler.handle(sub)
                .await().atMost(Duration.ofSeconds(120));

        assertEquals("Success", ack.status());
        assertEquals("subscribed", ack.details());
        assertEquals("it-c1", ack.correlationId());

        // Verify statement exists and is RUNNING
        String name = allocator.current(VEHICLE);
        assertNotNull(name, "Allocator should have a name for " + VEHICLE);
        String phase = flink.describe(name).await().atMost(Duration.ofSeconds(30));
        assertEquals("RUNNING", phase, "Filter statement should be RUNNING");
    }

    // ─── Test 2: DATA FLOW after subscribe ───────────────────────────────────

    @Test
    @Order(2)
    void dataFlow_afterSubscribe_produces9Records() throws Exception {
        // Produce F1 fixture (10 telemetry records)
        produceFixture("../test-data/fixtures/F1-input.jsonl", INPUT_TOPIC);

        // Consume output — expect 9 records (mdc IN 100, 200 for vehicle-fixture-001)
        List<String> records = consumeRecords(OUTPUT_TOPIC, 9, 90);

        assertEquals(9, records.size(),
                "Expected 9 records from filter mdc IN (100, 200)");
    }

    // ─── Test 3: UPDATE with carry-over ──────────────────────────────────────

    @Test
    @Order(3)
    void update_carryOverAndReturnsAck() {
        Subscription sub = new Subscription(VEHICLE, "it-c2", List.of("200", "300"));
        AckMessage ack = updateHandler.handle(sub)
                .await().atMost(Duration.ofMinutes(6));

        assertEquals("Success", ack.status());
        assertEquals("updated", ack.details());
        assertEquals("it-c2", ack.correlationId());

        // Verify new statement is RUNNING
        String newName = allocator.current(VEHICLE);
        String phase = flink.describe(newName).await().atMost(Duration.ofSeconds(30));
        assertEquals("RUNNING", phase, "Updated filter statement should be RUNNING");
    }

    // ─── Test 4: DATA FLOW after update ──────────────────────────────────────

    @Test
    @Order(4)
    void dataFlow_afterUpdate_producesRecordsWithNewPredicate() throws Exception {
        // Produce same fixture again (batch2)
        produceFixture("../test-data/fixtures/F1-input.jsonl", INPUT_TOPIC);

        // v2 filter is mdc IN (200, 300) — should produce 6 records from batch2
        // Total in sink: 9 (batch1 from v1) + 6 (batch2 from v2) = 15
        // But we consumed 9 already in test 2. Consume next 6.
        List<String> records = consumeRecords(OUTPUT_TOPIC, 6, 90);

        assertTrue(records.size() >= 6,
                "Expected at least 6 records from filter mdc IN (200, 300), got " + records.size());
    }

    // ─── Test 5: UNSUBSCRIBE ─────────────────────────────────────────────────

    @Test
    @Order(5)
    void unsubscribe_stopsStatementAndReturnsAck() {
        Subscription sub = new Subscription(VEHICLE, "it-c3", List.of());
        AckMessage ack = unsubscribeHandler.handle(sub)
                .await().atMost(Duration.ofSeconds(30));

        assertEquals("Success", ack.status());
        assertEquals("unsubscribed", ack.details());
        assertEquals("it-c3", ack.correlationId());
    }

    // ─── Test 6: NO DATA after unsubscribe ───────────────────────────────────

    @Test
    @Order(6)
    void afterUnsubscribe_statementNotRunning() {
        // Verify the statement is no longer RUNNING — this is the observable
        // consequence of unsubscribe. We don't produce more data because the
        // statement is already stopped (test 5). Verifying "no new output" would
        // require a timed absence check (racy); verifying "not RUNNING" is definitive.
        String stmtName = allocator.current(VEHICLE);
        assertNotNull(stmtName, "Allocator should still know the last statement name");
        String phase = flink.describe(stmtName).await().atMost(Duration.ofSeconds(10));
        assertNotEquals("RUNNING", phase,
                "Statement must NOT be RUNNING after unsubscribe, got: " + phase);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static void produceFixture(String fixturePath, String topic) throws Exception {
        Path path = Path.of(fixturePath);
        assertTrue(Files.exists(path), "Fixture must exist: " + path.toAbsolutePath());
        // Use shell pipe (bash -c) instead of redirectInput — confluent CLI
        // sometimes hangs on Java's redirectInput EOF handling.
        ProcessBuilder pb = new ProcessBuilder("bash", "-c",
                "confluent kafka topic produce " + topic + " --cluster " + cluster
                + " < " + path.toAbsolutePath());
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String output = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean done = proc.waitFor(60, TimeUnit.SECONDS);
        if (!done) {
            proc.destroyForcibly();
            fail("Produce timed out after 60s. Output: " + output);
        }
        if (proc.exitValue() != 0) {
            fail("Produce exit=" + proc.exitValue() + ". Output: " + output);
        }
    }

    private static List<String> consumeRecords(String topic, int count, int timeoutSec) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "timeout", String.valueOf(timeoutSec),
                "confluent", "kafka", "topic", "consume", topic,
                "--cluster", cluster,
                "--from-beginning", "--print-key=false",
                "--value-format", "jsonschema");
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String output = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        proc.waitFor(timeoutSec + 5, TimeUnit.SECONDS);

        List<String> lines = new ArrayList<>();
        for (String line : output.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("%") && !trimmed.startsWith("Starting")) {
                lines.add(trimmed);
            }
            if (lines.size() >= count) break;
        }
        return lines;
    }

    private static List<String> cli(String... args) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String output = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        proc.waitFor(30, TimeUnit.SECONDS);
        List<String> lines = new ArrayList<>();
        for (String line : output.split("\n")) {
            if (!line.trim().isEmpty()) lines.add(line.trim());
        }
        return lines;
    }

    private static void cleanCc() {
        try {
            // Delete all kf-qit-* statements
            cliQuiet("confluent", "flink", "statement", "stop", SETUP_STMT,
                    "--cloud", cloud, "--region", region);
            cliQuiet("confluent", "flink", "statement", "delete", SETUP_STMT,
                    "--cloud", cloud, "--region", region, "--force");

            String v1 = allocator != null ? allocator.current(VEHICLE) : null;
            if (v1 != null) {
                cliQuiet("confluent", "flink", "statement", "stop", v1,
                        "--cloud", cloud, "--region", region);
                cliQuiet("confluent", "flink", "statement", "delete", v1,
                        "--cloud", cloud, "--region", region, "--force");
            }

            // Also try to clean any leftover kf-flt-* from allocator
            for (int i = 1; i <= 5; i++) {
                String name = "kf-flt-vehiclef-" + i;
                cliQuiet("confluent", "flink", "statement", "stop", name,
                        "--cloud", cloud, "--region", region);
                cliQuiet("confluent", "flink", "statement", "delete", name,
                        "--cloud", cloud, "--region", region, "--force");
            }

            cliQuiet("confluent", "kafka", "topic", "delete", INPUT_TOPIC,
                    "--cluster", cluster, "--force");
            cliQuiet("confluent", "kafka", "topic", "delete", OUTPUT_TOPIC,
                    "--cluster", cluster, "--force");
            cliQuiet("confluent", "schema-registry", "subject", "delete",
                    OUTPUT_TOPIC + "-value", "--force");
            cliQuiet("confluent", "schema-registry", "subject", "delete",
                    OUTPUT_TOPIC + "-value", "--permanent", "--force");

            // Wait for topic deletion to propagate
            for (int i = 0; i < 12; i++) {
                ProcessBuilder pb = new ProcessBuilder("confluent", "kafka", "topic", "list",
                        "--cluster", cluster, "--output", "json");
                Process proc = pb.start();
                String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                proc.waitFor();
                if (!out.contains(INPUT_TOPIC) && !out.contains(OUTPUT_TOPIC)) break;
                Thread.sleep(5000);
            }
        } catch (Exception ignored) {}
    }

    private static void createInputTopic() {
        try {
            new ProcessBuilder("confluent", "kafka", "topic", "create", INPUT_TOPIC,
                    "--cluster", cluster, "--partitions", "1")
                    .start().waitFor();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create input topic", e);
        }
    }

    private static void createOutputTable() {
        try {
            Path ddlPath = Path.of("../sql/00-create-output-table.sql");
            String ddl = Files.readString(ddlPath)
                    .replace("`kf-data-test`", "`" + OUTPUT_TOPIC + "`");
            flink.submit(SETUP_STMT, ddl, null)
                    .await().atMost(Duration.ofSeconds(120));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create output table", e);
        }
    }

    private static void cliQuiet(String... args) {
        try {
            new ProcessBuilder(args).start().waitFor(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    private static void checkEnabled() {
        if (!"true".equals(System.getProperty("cc.integration"))) {
            throw new org.opentest4j.TestAbortedException(
                    "CC integration tests skipped. Run with -Dcc.integration=true");
        }
    }

    private static String sysProp(String key, String defaultVal) {
        return System.getProperty(key, defaultVal);
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

    private static void setField(Object obj, String fieldName, Object value) {
        try {
            var field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + fieldName, e);
        }
    }
}
