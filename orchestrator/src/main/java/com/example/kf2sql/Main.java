package com.example.kf2sql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * V1 PoC entry point. Two modes:
 *
 * <ul>
 *   <li>{@code --mode file} (default): read ONE subscription JSON from disk, run the
 *       orchestrator, print the ACK to stdout, exit. Useful for fixture-driven debugging.</li>
 *   <li>{@code --mode kafka}: poll the subscription topic, dispatch each message through the
 *       orchestrator, write ACKs to the ack topic. Polls until {@code --max-messages} or
 *       {@code --max-poll-empty} consecutive empty polls. Then exits.</li>
 * </ul>
 *
 * Common args:
 * <pre>
 * --template     path/to/01-filter-template.sql
 * --state        path/to/state.json
 * --input-topic  kf-input-test
 * --output-topic kf-data-test
 * --compute-pool lfcp-kknvdm
 * --cluster      lkc-6w3rv2
 * --environment  env-nvv5xz
 * --cloud        aws
 * --region       eu-central-1
 * </pre>
 *
 * File-mode args:
 * <pre>
 * --mode file
 * --subscription path/to/F1-subscription.json
 * </pre>
 *
 * Kafka-mode args:
 * <pre>
 * --mode kafka
 * --kafka-props        path/to/ccloud.properties
 * --subscription-topic kf-sub-test
 * --ack-topic          kf-ack-test
 * --group-id           kafka-variant-orchestrator
 * --max-messages       10        # exit after N messages handled
 * --max-poll-empty     6         # exit after N consecutive empty polls
 * </pre>
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        require(opts, "template", "state",
            "input-topic", "output-topic",
            "compute-pool", "cluster", "environment", "cloud", "region");

        OrchestratorLoop loop = buildLoop(opts);

        String mode = opts.getOrDefault("mode", "file");
        switch (mode) {
            case "file"  -> runFileMode(opts, loop);
            case "kafka" -> runKafkaMode(opts, loop);
            default      -> {
                System.err.println("--mode must be file or kafka, got: " + mode);
                System.exit(2);
            }
        }
    }

    // ─── File mode ────────────────────────────────────────────────────────────

    private static void runFileMode(Map<String, String> opts, OrchestratorLoop loop) throws Exception {
        require(opts, "subscription");
        Subscription sub = Subscription.fromJson(Files.readString(Path.of(opts.get("subscription"))));
        AckMessage ack = loop.handle(sub);
        System.out.println(ack.toJson());
        System.exit("Success".equals(ack.status()) ? 0 : 1);
    }

    // ─── Kafka mode ───────────────────────────────────────────────────────────

    private static void runKafkaMode(Map<String, String> opts, OrchestratorLoop loop) throws Exception {
        require(opts, "kafka-props", "subscription-topic", "ack-topic", "group-id");
        Properties props = KafkaConfig.load(Path.of(opts.get("kafka-props")));
        int maxMessages   = Integer.parseInt(opts.getOrDefault("max-messages",   "10"));
        int maxPollEmpty  = Integer.parseInt(opts.getOrDefault("max-poll-empty", "6"));

        try (var consumer = new SubscriptionConsumer(
                props, opts.get("subscription-topic"), opts.get("group-id"), Duration.ofSeconds(2));
             var producer = new AckProducer(props, opts.get("ack-topic"))) {

            int handled = 0;
            int emptyStreak = 0;
            while (handled < maxMessages && emptyStreak < maxPollEmpty) {
                final int[] count = {0};
                consumer.runOnce(sub -> {
                    try {
                        AckMessage ack = loop.handle(sub);
                        producer.send(ack);
                        System.err.println("[orchestrator] " + ack.toJson());
                        count[0]++;
                    } catch (Exception ex) {
                        AckMessage err = AckMessage.error(sub.correlationId(), ex.getMessage());
                        producer.send(err);
                        System.err.println("[orchestrator] " + err.toJson());
                        count[0]++;
                    }
                });
                if (count[0] == 0) {
                    emptyStreak++;
                } else {
                    handled += count[0];
                    emptyStreak = 0;
                }
            }
            System.err.println("[orchestrator] exiting: handled=" + handled
                + " emptyStreak=" + emptyStreak);
        }
    }

    // ─── Plumbing ─────────────────────────────────────────────────────────────

    private static OrchestratorLoop buildLoop(Map<String, String> opts) throws Exception {
        SqlGenerator sqlGen = new SqlGenerator(Files.readString(Path.of(opts.get("template"))));
        StatementNameAllocator allocator = new StatementNameAllocator(Path.of(opts.get("state")));
        FlinkLifecycle lifecycle = new FlinkLifecycle(
            new RealProcessRunner(),
            new FlinkLifecycle.Config(
                opts.get("compute-pool"),
                opts.get("cluster"),
                opts.get("environment"),
                opts.get("cloud"),
                opts.get("region")));
        return new OrchestratorLoop(
            sqlGen, allocator, lifecycle,
            opts.get("input-topic"), opts.get("output-topic"),
            /* waitForRunningTimeoutMs= */ 5 * 60 * 1000L,
            /* pollIntervalMs= */ 5_000L);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                map.put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        return map;
    }

    private static void require(Map<String, String> opts, String... keys) {
        for (String k : keys) {
            if (!opts.containsKey(k)) {
                System.err.println("Missing required arg: --" + k);
                System.exit(2);
            }
        }
    }
}
