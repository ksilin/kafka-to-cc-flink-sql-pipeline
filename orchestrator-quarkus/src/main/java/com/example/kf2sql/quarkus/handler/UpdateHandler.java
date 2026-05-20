package com.example.kf2sql.quarkus.handler;

import com.example.kf2sql.quarkus.*;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;

@ApplicationScoped
public class UpdateHandler {

    @Inject FlinkLifecycle flink;
    @Inject StatementNameAllocator allocator;
    @Inject SqlGenerator sqlGenerator;

    @ConfigProperty(name = "kf.input-topic") String inputTopic;
    @ConfigProperty(name = "kf.output-topic") String outputTopic;

    public Uni<AckMessage> handle(Subscription sub) {
        String previousName = allocator.current(sub.vehicleId());
        String newName = allocator.next(sub.vehicleId());
        String sql = sqlGenerator.fromSubscription(sub, inputTopic, outputTopic);

        Log.infof("UPDATE %s → %s (carry-over from %s)", sub.vehicleId(), newName, previousName);

        return flink.submit(newName, sql, previousName)
                .chain(() -> flink.stop(previousName))
                .chain(() -> flink.waitForRunning(newName, Duration.ofMinutes(5)))
                .replaceWith(AckMessage.success(sub.correlationId(), "updated"));
    }
}
