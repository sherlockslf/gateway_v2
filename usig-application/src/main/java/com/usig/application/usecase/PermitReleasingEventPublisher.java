package com.usig.application.usecase;

import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.domain.event.GatewayEvent;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

final class PermitReleasingEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final Flow.Publisher<GatewayEvent> upstream;
  private final RunConcurrencyLimiter.Permit permit;

  PermitReleasingEventPublisher(
      Flow.Publisher<GatewayEvent> upstream, RunConcurrencyLimiter.Permit permit) {
    this.upstream = Objects.requireNonNull(upstream, "upstream must not be null");
    this.permit = Objects.requireNonNull(permit, "permit must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    upstream.subscribe(new ReleasingSubscriber(subscriber));
  }

  private final class ReleasingSubscriber
      implements Flow.Subscriber<GatewayEvent>, Flow.Subscription {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicBoolean released = new AtomicBoolean();
    private Flow.Subscription upstreamSubscription;

    private ReleasingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstreamSubscription = subscription;
      downstream.onSubscribe(this);
    }

    @Override
    public void request(long n) {
      upstreamSubscription.request(n);
    }

    @Override
    public void cancel() {
      try {
        upstreamSubscription.cancel();
      } finally {
        release();
      }
    }

    @Override
    public void onNext(GatewayEvent item) {
      downstream.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
      try {
        downstream.onError(throwable);
      } finally {
        release();
      }
    }

    @Override
    public void onComplete() {
      try {
        downstream.onComplete();
      } finally {
        release();
      }
    }

    private void release() {
      if (released.compareAndSet(false, true)) {
        permit.close();
      }
    }
  }
}
