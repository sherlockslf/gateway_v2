package com.usig.application.memory;

import com.usig.application.port.ReplayStore;
import com.usig.application.port.RunEventPublisher;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;

public final class ReliableRunEventPublisher implements RunEventPublisher {

  private final Map<RunId, Flow.Publisher<GatewayEvent>> livePublishers = new ConcurrentHashMap<>();
  private final ReplayStore replayStore;
  private final ReliabilityOptions options;

  public ReliableRunEventPublisher(ReplayStore replayStore, ReliabilityOptions options) {
    this.replayStore = Objects.requireNonNull(replayStore, "replayStore must not be null");
    this.options = Objects.requireNonNull(options, "options must not be null");
  }

  @Override
  public void attach(RunId runId, Flow.Publisher<GatewayEvent> events) {
    livePublishers.put(
        runId, new ReliableGatewayEventPublisher(runId, events, replayStore, options));
  }

  @Override
  public Flow.Publisher<GatewayEvent> subscribe(RunId runId, String lastEventId) {
    if (lastEventId != null && !lastEventId.isBlank()) {
      return replayStore.readAfter(runId, lastEventId);
    }
    return livePublishers.getOrDefault(runId, new ListGatewayEventPublisher(java.util.List.of()));
  }
}
