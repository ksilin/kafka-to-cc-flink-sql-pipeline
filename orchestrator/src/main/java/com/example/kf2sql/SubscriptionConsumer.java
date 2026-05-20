package com.example.kf2sql;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Kafka consumer that polls the subscription topic and feeds {@link Subscription} records
 * to a callback. Single-threaded, blocking poll loop. V1 PoC.
 *
 * <p>Subscription topic is raw JSON (no Schema Registry per CONTRACT §5). Values are
 * deserialized via {@link StringDeserializer}, then parsed via {@link Subscription#fromJson}.
 *
 * <p>For tests, see {@link SubscriptionConsumer#runOnce(Consumer)} — drains one poll cycle
 * and returns. Production code uses {@link SubscriptionConsumer#runForever(Consumer)} which
 * blocks until {@link #close()}.
 */
public class SubscriptionConsumer implements Closeable {

    private final KafkaConsumer<String, String> kc;
    private final Duration pollTimeout;
    private volatile boolean running = true;

    public SubscriptionConsumer(Properties baseProps, String topic, String groupId, Duration pollTimeout) {
        Properties p = new Properties();
        p.putAll(baseProps);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.kc = new KafkaConsumer<>(p);
        this.kc.subscribe(Collections.singletonList(topic));
        this.pollTimeout = pollTimeout;
    }

    /** Drain one poll cycle. Returns the number of records processed. */
    public int runOnce(Consumer<Subscription> handler) {
        ConsumerRecords<String, String> records = kc.poll(pollTimeout);
        int n = 0;
        for (ConsumerRecord<String, String> r : records) {
            handler.accept(Subscription.fromJson(r.value()));
            n++;
        }
        return n;
    }

    /** Block until {@link #close()} is called or the thread is interrupted. */
    public void runForever(Consumer<Subscription> handler) {
        while (running && !Thread.currentThread().isInterrupted()) {
            runOnce(handler);
        }
    }

    @Override
    public void close() {
        running = false;
        kc.close();
    }
}
