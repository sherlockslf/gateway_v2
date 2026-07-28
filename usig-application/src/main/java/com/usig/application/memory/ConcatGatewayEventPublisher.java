package com.usig.application.memory;

import com.usig.domain.event.GatewayEvent;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class ConcatGatewayEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final Flow.Publisher<GatewayEvent> first;
  private final Flow.Publisher<GatewayEvent> second;

  public ConcatGatewayEventPublisher(
      Flow.Publisher<GatewayEvent> first, Flow.Publisher<GatewayEvent> second) {
    this.first = Objects.requireNonNull(first, "first must not be null");
    this.second = Objects.requireNonNull(second, "second must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    first.subscribe(new FirstSubscriber(subscriber));
  }

  private final class FirstSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private long requested;
    private Flow.Subscription current;
    private boolean secondSubscribed;
    private boolean canceled;

    private FirstSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      current = subscription;
      downstream.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long n) {
              if (n <= 0 || canceled) {
                return;
              }
              requested = saturatedAdd(requested, n);
              current.request(n);
            }

            @Override
            public void cancel() {
              canceled = true;
              current.cancel();
            }
          });
    }

    @Override
    public void onNext(GatewayEvent item) {
      if (canceled) {
        return;
      }
      if (requested > 0) {
        requested--;
      }
      downstream.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
      if (!canceled) {
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      if (canceled || secondSubscribed) {
        return;
      }
      secondSubscribed = true;
      second.subscribe(
          new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
              current = subscription;
              if (requested > 0) {
                subscription.request(requested);
              }
            }

            @Override
            public void onNext(GatewayEvent item) {
              FirstSubscriber.this.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
              FirstSubscriber.this.onError(throwable);
            }

            @Override
            public void onComplete() {
              if (!canceled) {
                downstream.onComplete();
              }
            }
          });
    }
  }

  private static long saturatedAdd(long left, long right) {
    long sum = left + right;
    if (sum < 0) {
      return Long.MAX_VALUE;
    }
    return sum;
  }
}
