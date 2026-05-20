package com.example.kf2sql.quarkus;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletionStage;

/**
 * Reactive ACK producer. Pattern: mirrors {@code StatusProducer} from upstream service.
 * Key = correlationId for per-subscription ordering.
 */
@ApplicationScoped
public class AckProducer {

    private final MutinyEmitter<AckMessage> emitter;

    @Inject
    public AckProducer(@Channel("acks") MutinyEmitter<AckMessage> emitter) {
        this.emitter = emitter;
    }

    public CompletionStage<Void> send(AckMessage ack) {
        Log.infof("Sending ACK: %s", ack);
        return emitter
                .sendMessage(
                        Message.of(ack)
                                .addMetadata(
                                        OutgoingKafkaRecordMetadata.<String>builder()
                                                .withKey(ack.correlationId())
                                                .build()))
                .subscribeAsCompletionStage();
    }
}
