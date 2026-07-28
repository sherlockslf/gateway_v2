package com.usig.infra.messaging;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

public final class QuarkusKafkaSideEventSender implements KafkaSideEventSender {

  private final Emitter<KafkaSideEventRecord> emitter;

  public QuarkusKafkaSideEventSender(Emitter<KafkaSideEventRecord> emitter) {
    this.emitter = Objects.requireNonNull(emitter, "emitter must not be null");
  }

  @Override
  public CompletionStage<Void> send(KafkaSideEventRecord record) {
    Objects.requireNonNull(record, "record must not be null");
    CompletableFuture<Void> completion = new CompletableFuture<>();
    OutgoingKafkaRecordMetadata<String> metadata =
        OutgoingKafkaRecordMetadata.<String>builder()
            .withTopic(record.topic())
            .withKey(record.key())
            .build();
    Message<KafkaSideEventRecord> message =
        Message.of(record)
            .addMetadata(metadata)
            .withAck(
                () -> {
                  completion.complete(null);
                  return CompletableFuture.completedFuture(null);
                })
            .withNack(
                reason -> {
                  completion.completeExceptionally(reason);
                  return CompletableFuture.completedFuture(null);
                });
    emitter.send(message);
    return completion;
  }
}
