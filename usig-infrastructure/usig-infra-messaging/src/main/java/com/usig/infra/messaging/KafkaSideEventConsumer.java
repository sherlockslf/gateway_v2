package com.usig.infra.messaging;

import com.usig.application.sideevent.IdempotentSideEventConsumer;
import com.usig.application.sideevent.SideEventConsumptionResult;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class KafkaSideEventConsumer {

  private final IdempotentSideEventConsumer consumer;

  public KafkaSideEventConsumer(IdempotentSideEventConsumer consumer) {
    this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
  }

  public CompletionStage<SideEventConsumptionResult> consume(KafkaSideEventRecord record) {
    Objects.requireNonNull(record, "record must not be null");
    return consumer.consume(record.toSideEvent());
  }
}
