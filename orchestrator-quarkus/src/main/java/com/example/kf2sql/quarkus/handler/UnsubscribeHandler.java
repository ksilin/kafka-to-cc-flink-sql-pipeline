package com.example.kf2sql.quarkus.handler;

import com.example.kf2sql.quarkus.*;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class UnsubscribeHandler {

    @Inject FlinkLifecycle flink;
    @Inject StatementNameAllocator allocator;

    public Uni<AckMessage> handle(Subscription sub) {
        String previous = allocator.current(sub.vehicleId());
        Log.infof("UNSUBSCRIBE %s (statement %s)", sub.vehicleId(), previous);
        if (previous != null) {
            return flink.stop(previous)
                    .replaceWith(AckMessage.success(sub.correlationId(), "unsubscribed"));
        }
        return Uni.createFrom().item(AckMessage.success(sub.correlationId(), "unsubscribed"));
    }
}
