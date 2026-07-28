package com.usig.application.usecase;

import com.usig.application.port.RunCircuitBreaker;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

final class CircuitBreakerRecordingEventPublisher implements Flow.Publisher<GatewayEvent> {

  private final Flow.Publisher<GatewayEvent> upstream;
  private final ServiceDescriptor service;
  private final RunCircuitBreaker circuitBreaker;

  CircuitBreakerRecordingEventPublisher(
      Flow.Publisher<GatewayEvent> upstream,
      ServiceDescriptor service,
      RunCircuitBreaker circuitBreaker) {
    this.upstream = Objects.requireNonNull(upstream, "upstream must not be null");
    this.service = Objects.requireNonNull(service, "service must not be null");
    this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    upstream.subscribe(new RecordingSubscriber(subscriber));
  }

  private final class RecordingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicBoolean recorded = new AtomicBoolean();

    private RecordingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(GatewayEvent item) {
      downstream.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
      if (recorded.compareAndSet(false, true)) {
        circuitBreaker.onFailure(service, throwable);
      }
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      if (recorded.compareAndSet(false, true)) {
        circuitBreaker.onSuccess(service);
      }
      downstream.onComplete();
    }
  }
}
