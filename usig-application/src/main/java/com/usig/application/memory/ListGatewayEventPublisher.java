package com.usig.application.memory;

import com.usig.domain.event.GatewayEvent;
import java.util.List;
import java.util.concurrent.Flow;

public final class ListGatewayEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final List<GatewayEvent> events;

  public ListGatewayEventPublisher(List<GatewayEvent> events) {
    this.events = List.copyOf(events);
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    subscriber.onSubscribe(
        new Flow.Subscription() {
          private int index;
          private boolean canceled;

          @Override
          public void request(long requested) {
            if (requested <= 0 || canceled) {
              return;
            }
            long emitted = 0;
            while (emitted < requested && index < events.size() && !canceled) {
              GatewayEvent event = events.get(index);
              index++;
              subscriber.onNext(event);
              emitted++;
            }
            if (index == events.size() && !canceled) {
              canceled = true;
              subscriber.onComplete();
            }
          }

          @Override
          public void cancel() {
            canceled = true;
          }
        });
  }
}
