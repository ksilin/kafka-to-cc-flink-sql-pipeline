package com.example.kf2sql;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Loads Kafka client configuration from a Java properties file.
 *
 * <p>Expected keys (CC standard SASL_SSL bundle):
 * <ul>
 *   <li>{@code bootstrap.servers}</li>
 *   <li>{@code security.protocol=SASL_SSL}</li>
 *   <li>{@code sasl.mechanism=PLAIN}</li>
 *   <li>{@code sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='K' password='S';}</li>
 * </ul>
 *
 * <p>For this project, props files live at {@code /home/ks/code/workspaces/ccloud.props/}.
 * E.g. {@code ccloud.lkc-6w3rv2.<KEY>.properties} once that key+secret are registered.
 *
 * <p>The configurer enriches with safe defaults (idempotence, acks=all, earliest).
 */
public final class KafkaConfig {

    private KafkaConfig() {}

    public static Properties load(Path propsFile) {
        Properties p = new Properties();
        try (var in = Files.newBufferedReader(propsFile)) {
            p.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return p;
    }
}
