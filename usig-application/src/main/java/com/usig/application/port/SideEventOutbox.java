package com.usig.application.port;

import com.usig.application.sideevent.SideEvent;
import com.usig.application.sideevent.SideEventOutboxEntry;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface SideEventOutbox {

  CompletionStage<Void> recordFailure(SideEvent event, Throwable failure, Instant failedAt);

  CompletionStage<List<SideEventOutboxEntry>> pending(int maxEntries, Instant now);

  CompletionStage<Void> markPublished(String eventId);

  CompletionStage<Void> markRetryFailure(
      String eventId, Throwable failure, Instant failedAt, Instant nextAttemptAt);
}
