package com.example.kf2sql.quarkus.handler;

import com.example.kf2sql.quarkus.*;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class SubscribeHandler {

    @Inject FlinkLifecycle flink;
    @Inject StatementNameAllocator allocator;
    @Inject SqlGenerator sqlGenerator;

    @ConfigProperty(name = "kf.input-topic") String inputTopic;
    @ConfigProperty(name = "kf.output-topic") String outputTopic;

    public Uni<AckMessage> handle(Subscription sub) {
        String name = allocator.next(sub.vehicleId());
        String sql = sqlGenerator.fromSubscription(sub, inputTopic, outputTopic);
        Log.infof("SUBSCRIBE %s → statement %s", sub.vehicleId(), name);
        return flink.submit(name, sql, null)
                .replaceWith(AckMessage.success(sub.correlationId(), "subscribed"));
    }
}
