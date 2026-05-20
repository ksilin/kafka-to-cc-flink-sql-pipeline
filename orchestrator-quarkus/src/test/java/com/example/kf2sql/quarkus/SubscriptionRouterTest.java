package com.example.kf2sql.quarkus;

import com.example.kf2sql.quarkus.handler.SubscribeHandler;
import com.example.kf2sql.quarkus.handler.UnsubscribeHandler;
import com.example.kf2sql.quarkus.handler.UpdateHandler;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for SubscriptionRouter dispatch logic. Uses stub handlers and
 * a no-op AckProducer (no Quarkus CDI needed).
 */
class SubscriptionRouterTest {

    private RecordingSubscribeHandler subscribeHandler;
    private RecordingUpdateHandler updateHandler;
    private RecordingUnsubscribeHandler unsubscribeHandler;
    private StatementNameAllocator allocator;
    private RecordingAckProducer ackProducer;
    private SubscriptionRouter router;

    @BeforeEach
    void setup() {
        subscribeHandler = new RecordingSubscribeHandler();
        updateHandler = new RecordingUpdateHandler();
        unsubscribeHandler = new RecordingUnsubscribeHandler();
        allocator = new StatementNameAllocator();
        ackProducer = new RecordingAckProducer();
        router = new SubscriptionRouter(
                subscribeHandler, updateHandler, unsubscribeHandler, allocator, ackProducer);
    }

    @Test
    void subscribe_firstTime_dispatchesToSubscribeHandler() {
        Subscription sub = new Subscription("V1", "c1", List.of("100", "200"));
        router.route(sub).await().indefinitely();

        assertEquals(1, subscribeHandler.calls.size());
        assertEquals(0, updateHandler.calls.size());
        assertEquals(0, unsubscribeHandler.calls.size());
        assertEquals(1, ackProducer.acks.size());
        assertEquals("subscribed", ackProducer.acks.get(0).details());
    }

    @Test
    void update_secondTime_dispatchesToUpdateHandler() {
        // Seed allocator
        allocator.next("V1");
        Subscription sub = new Subscription("V1", "c2", List.of("200", "300"));
        router.route(sub).await().indefinitely();

        assertEquals(0, subscribeHandler.calls.size());
        assertEquals(1, updateHandler.calls.size());
        assertEquals(1, ackProducer.acks.size());
        assertEquals("updated", ackProducer.acks.get(0).details());
    }

    @Test
    void unsubscribe_emptyList_dispatchesToUnsubscribeHandler() {
        Subscription sub = new Subscription("V1", "c3", List.of());
        router.route(sub).await().indefinitely();

        assertEquals(0, subscribeHandler.calls.size());
        assertEquals(1, unsubscribeHandler.calls.size());
        assertEquals(1, ackProducer.acks.size());
        assertEquals("unsubscribed", ackProducer.acks.get(0).details());
    }

    @Test
    void unsubscribe_withoutPriorSubscribe_isIdempotent() {
        Subscription sub = new Subscription("Vnew", "c4", List.of());
        router.route(sub).await().indefinitely();

        assertEquals(1, unsubscribeHandler.calls.size());
        assertEquals(1, ackProducer.acks.size());
        assertEquals("unsubscribed", ackProducer.acks.get(0).details());
        // No CLI calls expected (nothing to stop)
    }

    @Test
    void handlerFailure_producesErrorAck() {
        subscribeHandler.failNext = true;
        Subscription sub = new Subscription("V1", "c1", List.of("100"));
        router.route(sub).await().indefinitely();

        assertEquals(1, ackProducer.acks.size());
        assertEquals("Error", ackProducer.acks.get(0).status());
    }

    // ─── Stubs ───────────────────────────────────────────────────────────────

    static class RecordingSubscribeHandler extends SubscribeHandler {
        final List<Subscription> calls = new ArrayList<>();
        boolean failNext = false;

        RecordingSubscribeHandler() { super(); }

        @Override
        public Uni<AckMessage> handle(Subscription sub) {
            calls.add(sub);
            if (failNext) return Uni.createFrom().failure(new RuntimeException("test failure"));
            return Uni.createFrom().item(AckMessage.success(sub.correlationId(), "subscribed"));
        }
    }

    static class RecordingUpdateHandler extends UpdateHandler {
        final List<Subscription> calls = new ArrayList<>();

        RecordingUpdateHandler() { super(); }

        @Override
        public Uni<AckMessage> handle(Subscription sub) {
            calls.add(sub);
            return Uni.createFrom().item(AckMessage.success(sub.correlationId(), "updated"));
        }
    }

    static class RecordingUnsubscribeHandler extends UnsubscribeHandler {
        final List<Subscription> calls = new ArrayList<>();

        RecordingUnsubscribeHandler() { super(); }

        @Override
        public Uni<AckMessage> handle(Subscription sub) {
            calls.add(sub);
            return Uni.createFrom().item(AckMessage.success(sub.correlationId(), "unsubscribed"));
        }
    }

    static class RecordingAckProducer extends AckProducer {
        final List<AckMessage> acks = new ArrayList<>();

        RecordingAckProducer() { super(null); }

        @Override
        public CompletionStage<Void> send(AckMessage ack) {
            acks.add(ack);
            return CompletableFuture.completedFuture(null);
        }
    }
}
