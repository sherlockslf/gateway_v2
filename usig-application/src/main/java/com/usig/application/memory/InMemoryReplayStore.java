package com.usig.application.memory;

import com.usig.application.port.ReplayStore;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;

public final class InMemoryReplayStore implements ReplayStore {

  private final int maxEventsPerRun;
  private final int maxBytesPerRun;
  private final Map<RunId, ReplayWindow> windows = new ConcurrentHashMap<>();

  public InMemoryReplayStore(int maxEventsPerRun, int maxBytesPerRun) {
    if (maxEventsPerRun < 1) {
      throw new IllegalArgumentException("maxEventsPerRun must be positive");
    }
    if (maxBytesPerRun < 1) {
      throw new IllegalArgumentException("maxBytesPerRun must be positive");
    }
    this.maxEventsPerRun = maxEventsPerRun;
    this.maxBytesPerRun = maxBytesPerRun;
  }

  @Override
  public CompletionStage<Void> append(RunId runId, GatewayEvent event, Duration ttl) {
    Objects.requireNonNull(runId, "runId must not be null");
    Objects.requireNonNull(event, "event must not be null");
    windows
        .computeIfAbsent(runId, ignored -> new ReplayWindow(maxEventsPerRun, maxBytesPerRun))
        .append(event);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Flow.Publisher<GatewayEvent> readAfter(RunId runId, String lastEventId) {
    ReplayWindow window = windows.get(runId);
    if (window == null) {
      return errorPublisher(new UseCaseException(ErrorCode.CURSOR_EXPIRED));
    }
    return window.readAfter(lastEventId);
  }

  @Override
  public CompletionStage<Void> delete(RunId runId) {
    windows.remove(runId);
    return CompletableFuture.completedFuture(null);
  }

  private static Flow.Publisher<GatewayEvent> errorPublisher(Throwable throwable) {
    return subscriber ->
        subscriber.onSubscribe(
            new Flow.Subscription() {
              @Override
              public void request(long n) {
                subscriber.onError(throwable);
              }

              @Override
              public void cancel() {}
            });
  }

  private static final class ReplayWindow {

    private final int maxEvents;
    private final int maxBytes;
    private final List<GatewayEvent> events = new ArrayList<>();
    private int bytes;

    private ReplayWindow(int maxEvents, int maxBytes) {
      this.maxEvents = maxEvents;
      this.maxBytes = maxBytes;
    }

    private synchronized void append(GatewayEvent event) {
      events.add(event);
      bytes += estimateBytes(event);
      while (events.size() > maxEvents || bytes > maxBytes) {
        GatewayEvent removed = events.remove(0);
        bytes -= estimateBytes(removed);
      }
    }

    private synchronized Flow.Publisher<GatewayEvent> readAfter(String lastEventId) {
      int index = findIndex(lastEventId);
      if (index < 0) {
        return errorPublisher(new UseCaseException(ErrorCode.CURSOR_EXPIRED));
      }
      return new ListGatewayEventPublisher(events.subList(index + 1, events.size()));
    }

    private int findIndex(String lastEventId) {
      if (lastEventId == null || lastEventId.isBlank()) {
        return -1;
      }
      for (int i = 0; i < events.size(); i++) {
        if (events.get(i).eventId().equals(lastEventId)) {
          return i;
        }
      }
      return -1;
    }

    private static int estimateBytes(GatewayEvent event) {
      int bytes = event.eventId().length() + event.wireEventType().length();
      Object content = event.payload().get("content");
      if (content != null) {
        bytes += content.toString().length() * 2;
      }
      return bytes;
    }
  }
}
