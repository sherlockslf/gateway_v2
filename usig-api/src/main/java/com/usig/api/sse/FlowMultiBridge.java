package com.usig.api.sse;

import io.smallrye.mutiny.Multi;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class FlowMultiBridge {

  private FlowMultiBridge() {}

  public static <T> Multi<T> toMulti(Flow.Publisher<T> publisher) {
    return Multi.createFrom()
        .emitter(
            emitter -> {
              AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();
              publisher.subscribe(
                  new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                      subscriptionRef.set(subscription);
                      subscription.request(1);
                    }

                    @Override
                    public void onNext(T item) {
                      emitter.emit(item);
                      Flow.Subscription subscription = subscriptionRef.get();
                      if (subscription != null) {
                        subscription.request(1);
                      }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                      emitter.fail(throwable);
                    }

                    @Override
                    public void onComplete() {
                      emitter.complete();
                    }
                  });
              emitter.onTermination(
                  () -> {
                    Flow.Subscription subscription = subscriptionRef.get();
                    if (subscription != null) {
                      subscription.cancel();
                    }
                  });
            });
  }
}
