package com.usig.application.memory;

import com.usig.domain.event.GatewayEvent;
import java.util.List;
import java.util.concurrent.Flow;

public final class StaticGatewayEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final ListGatewayEventPublisher delegate;

  public StaticGatewayEventPublisher(List<GatewayEvent> events) {
    this.delegate = new ListGatewayEventPublisher(events);
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    delegate.subscribe(subscriber);
  }
}
