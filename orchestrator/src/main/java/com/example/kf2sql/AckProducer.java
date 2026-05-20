package com.example.kf2sql;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka producer for ACK messages. Topic schema is raw JSON per CONTRACT §4.
 *
 * <p>Key = {@code correlationId}, value = ACK JSON, ordering preserved per
 * correlationId on the partition assigned by the default partitioner.
 *
 * <p>V1: synchronous send (blocks until ack from broker). Sub-second per send for our load
 * model. V2 with high throughput would switch to async + linger batching.
 */
public class AckProducer implements Closeable {

    private final KafkaProducer<String, String> kp;
    private final String topic;

    public AckProducer(Properties baseProps, String topic) {
        Properties p = new Properties();
        p.putAll(baseProps);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        p.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.kp = new KafkaProducer<>(p);
        this.topic = topic;
    }

    public void send(AckMessage ack) {
        try {
            kp.send(new ProducerRecord<>(topic, ack.correlationId(), ack.toJson())).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("ACK send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("ACK send failed: " + ack.toJson(), e);
        }
    }

    @Override
    public void close() {
        kp.flush();
        kp.close();
    }
}
