package com.usig.infra.messaging;

import com.usig.application.port.SideEventPublisher;
import com.usig.application.sideevent.SideEvent;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class KafkaSideEventPublisher implements SideEventPublisher {

  private final KafkaSideEventSender sender;

  public KafkaSideEventPublisher(KafkaSideEventSender sender) {
    this.sender = Objects.requireNonNull(sender, "sender must not be null");
  }

  @Override
  public CompletionStage<Void> publish(SideEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    return sender.send(KafkaSideEventRecord.fromSideEvent(event));
  }
}
