package com.example.kf2sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link KafkaConfig}. {@link AckProducer} and {@link SubscriptionConsumer}
 * are integration concerns (need a real broker); covered by Phase 08.1's IT and demo runner.
 *
 * Why no unit test for AckProducer/SubscriptionConsumer here: KafkaProducer/Consumer
 * cannot be reasonably stubbed without an embedded broker (testcontainers /
 * EmbeddedKafkaBroker). For V1 PoC scope, we rely on the demo runner (08.2) to validate
 * end-to-end against real Kafka.
 */
class KafkaConfigTest {

    @Test
    void load_readsPropertiesFile(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("k.properties");
        Files.writeString(file, """
            bootstrap.servers=pkc-test.example.com:9092
            security.protocol=SASL_SSL
            sasl.mechanism=PLAIN
            sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='k' password='s';
            """);

        Properties p = KafkaConfig.load(file);

        assertEquals("pkc-test.example.com:9092", p.getProperty("bootstrap.servers"));
        assertEquals("SASL_SSL", p.getProperty("security.protocol"));
        assertEquals("PLAIN", p.getProperty("sasl.mechanism"));
        assertTrue(p.getProperty("sasl.jaas.config").contains("username='k'"));
    }

    @Test
    void load_missingFileThrows() {
        assertThrows(RuntimeException.class,
            () -> KafkaConfig.load(Path.of("/nonexistent/k.properties")));
    }
}
