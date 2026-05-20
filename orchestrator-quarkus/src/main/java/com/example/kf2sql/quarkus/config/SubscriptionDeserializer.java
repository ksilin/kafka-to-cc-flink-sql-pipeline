package com.example.kf2sql.quarkus.config;

import com.example.kf2sql.quarkus.Subscription;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class SubscriptionDeserializer extends ObjectMapperDeserializer<Subscription> {
    public SubscriptionDeserializer() {
        super(Subscription.class);
    }
}
