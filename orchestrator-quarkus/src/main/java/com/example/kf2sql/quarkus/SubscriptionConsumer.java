package com.example.kf2sql.quarkus;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Reactive Kafka consumer for subscription messages.
 * Pattern: mirrors {@code CommandConsumer} from upstream deployment service.
 *
 * <p>SmallRye Reactive Messaging handles consumer lifecycle, offset management,
 * deserialization, and backpressure. No manual poll loop.
 */
@Startup
@ApplicationScoped
public class SubscriptionConsumer {

    private final SubscriptionRouter router;

    @Inject
    public SubscriptionConsumer(SubscriptionRouter router) {
        this.router = router;
    }

    @Incoming("subscriptions")
    public Uni<Void> consume(Message<Subscription> message) {
        Log.infof("Received subscription: %s", message.getPayload());
        return Uni.createFrom()
                .item(message.getPayload())
                .onItem().ifNotNull()
                .transformToUni(router::route)
                .eventually(() -> Uni.createFrom().completionStage(message::ack))
                .onFailure()
                .invoke(e -> Log.errorf(e, "Failed to process subscription"))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .replaceWithVoid();
    }
}
