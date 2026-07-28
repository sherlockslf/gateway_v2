package com.usig.application.memory;

import com.usig.application.port.RunEventPublisher;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

public final class InMemoryRunEventPublisher implements RunEventPublisher, AutoCloseable {

  private final Map<RunId, Flow.Publisher<GatewayEvent>> publishers = new ConcurrentHashMap<>();

  @Override
  public void attach(RunId runId, Flow.Publisher<GatewayEvent> events) {
    publishers.put(runId, events);
  }

  @Override
  public Flow.Publisher<GatewayEvent> subscribe(RunId runId, String lastEventId) {
    return publishers.getOrDefault(runId, emptyPublisher());
  }

  private static Flow.Publisher<GatewayEvent> emptyPublisher() {
    return subscriber -> {
      SubmissionPublisher<GatewayEvent> publisher = new SubmissionPublisher<>();
      publisher.subscribe(subscriber);
      publisher.close();
    };
  }

  @Override
  public void close() {
    publishers.clear();
  }
}
