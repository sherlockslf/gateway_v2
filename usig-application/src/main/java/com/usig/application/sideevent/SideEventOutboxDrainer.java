package com.usig.application.sideevent;

import com.usig.application.port.SideEventOutbox;
import com.usig.application.port.SideEventPublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class SideEventOutboxDrainer {

  private final SideEventOutbox outbox;
  private final SideEventPublisher publisher;
  private final SideEventOutboxDrainPolicy policy;
  private final Clock clock;

  public SideEventOutboxDrainer(
      SideEventOutbox outbox,
      SideEventPublisher publisher,
      SideEventOutboxDrainPolicy policy,
      Clock clock) {
    this.outbox = Objects.requireNonNull(outbox, "outbox must not be null");
    this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  public CompletionStage<SideEventOutboxDrainResult> drainOnce() {
    Instant now = clock.instant();
    return outbox.pending(policy.maxBatchSize(), now).thenCompose(entries -> drain(entries, now));
  }

  private CompletionStage<SideEventOutboxDrainResult> drain(
      List<SideEventOutboxEntry> entries, Instant now) {
    if (entries.isEmpty()) {
      return CompletableFuture.completedFuture(SideEventOutboxDrainResult.empty());
    }
    CompletableFuture<SideEventOutboxDrainResult> result = new CompletableFuture<>();
    drainNext(entries, 0, 0, 0, now, result);
    return result;
  }

  private void drainNext(
      List<SideEventOutboxEntry> entries,
      int index,
      int published,
      int failed,
      Instant now,
      CompletableFuture<SideEventOutboxDrainResult> result) {
    if (index >= entries.size()) {
      result.complete(new SideEventOutboxDrainResult(entries.size(), published, failed));
      return;
    }
    SideEventOutboxEntry entry = entries.get(index);
    CompletionStage<Void> sent;
    try {
      sent = publisher.publish(entry.toSideEvent());
    } catch (RuntimeException exception) {
      markFailure(entries, index, published, failed, now, result, entry, exception);
      return;
    }
    sent.whenComplete(
        (ignored, throwable) -> {
          if (throwable == null) {
            outbox
                .markPublished(entry.eventId())
                .whenComplete(
                    (removed, removeFailure) -> {
                      if (removeFailure == null) {
                        drainNext(entries, index + 1, published + 1, failed, now, result);
                      } else {
                        result.completeExceptionally(removeFailure);
                      }
                    });
          } else {
            markFailure(entries, index, published, failed, now, result, entry, throwable);
          }
        });
  }

  private void markFailure(
      List<SideEventOutboxEntry> entries,
      int index,
      int published,
      int failed,
      Instant now,
      CompletableFuture<SideEventOutboxDrainResult> result,
      SideEventOutboxEntry entry,
      Throwable throwable) {
    outbox
        .markRetryFailure(entry.eventId(), throwable, now, now.plus(policy.retryDelay()))
        .whenComplete(
            (ignored, markFailure) -> {
              if (markFailure == null) {
                drainNext(entries, index + 1, published, failed + 1, now, result);
              } else {
                result.completeExceptionally(markFailure);
              }
            });
  }
}
