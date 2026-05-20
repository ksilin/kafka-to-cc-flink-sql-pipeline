package com.example.kf2sql.quarkus;

import com.example.kf2sql.quarkus.handler.SubscribeHandler;
import com.example.kf2sql.quarkus.handler.UnsubscribeHandler;
import com.example.kf2sql.quarkus.handler.UpdateHandler;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Routes subscriptions to typed handlers.
 * Pattern: mirrors {@code CommandRouter} from upstream service, but simpler —
 * dispatch is by subscription state (new / update / unsubscribe), not by a
 * (ConfigurationType, CommandType) matrix.
 */
@ApplicationScoped
public class SubscriptionRouter {

    private final SubscribeHandler subscribeHandler;
    private final UpdateHandler updateHandler;
    private final UnsubscribeHandler unsubscribeHandler;
    private final StatementNameAllocator allocator;
    private final AckProducer ackProducer;

    @Inject
    public SubscriptionRouter(
            SubscribeHandler subscribeHandler,
            UpdateHandler updateHandler,
            UnsubscribeHandler unsubscribeHandler,
            StatementNameAllocator allocator,
            AckProducer ackProducer) {
        this.subscribeHandler = subscribeHandler;
        this.updateHandler = updateHandler;
        this.unsubscribeHandler = unsubscribeHandler;
        this.allocator = allocator;
        this.ackProducer = ackProducer;
    }

    public Uni<Void> route(Subscription sub) {
        return Uni.createFrom().item(() -> {
                    if (sub.isUnsubscribe()) return "unsubscribe";
                    return allocator.current(sub.vehicleId()) == null ? "subscribe" : "update";
                })
                .chain(action -> {
                    Log.infof("Routing %s for vehicle=%s", action, sub.vehicleId());
                    return switch (action) {
                        case "subscribe" -> subscribeHandler.handle(sub);
                        case "update" -> updateHandler.handle(sub);
                        case "unsubscribe" -> unsubscribeHandler.handle(sub);
                        default -> Uni.createFrom().failure(
                                new IllegalStateException("Unknown action: " + action));
                    };
                })
                .chain(ack -> Uni.createFrom().completionStage(() -> ackProducer.send(ack)))
                .onFailure().recoverWithUni(e -> {
                    Log.errorf(e, "Subscription handling failed for vehicle=%s", sub.vehicleId());
                    AckMessage errorAck = AckMessage.error(sub.correlationId(), e.getMessage());
                    return Uni.createFrom().completionStage(() -> ackProducer.send(errorAck));
                });
    }
}
