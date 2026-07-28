package com.usig.domain.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class RunStatusTest {

  @Test
  void allowsDocumentedLifecycleTransitions() {
    assertTrue(RunStatus.ACCEPTED.canTransitionTo(RunStatus.STARTING));
    assertTrue(RunStatus.STARTING.canTransitionTo(RunStatus.RUNNING));
    assertTrue(RunStatus.STARTING.canTransitionTo(RunStatus.CANCEL_REQUESTED));
    assertTrue(RunStatus.RUNNING.canTransitionTo(RunStatus.WAITING_INPUT));
    assertTrue(RunStatus.WAITING_INPUT.canTransitionTo(RunStatus.RUNNING));
    assertTrue(RunStatus.RUNNING.canTransitionTo(RunStatus.CANCEL_REQUESTED));
    assertTrue(RunStatus.CANCEL_REQUESTED.canTransitionTo(RunStatus.CANCELED));
    assertTrue(RunStatus.CANCEL_REQUESTED.canTransitionTo(RunStatus.COMPLETED));
  }

  @Test
  void rejectsTransitionAfterTerminalStatus() {
    assertFalse(RunStatus.COMPLETED.canTransitionTo(RunStatus.RUNNING));
    assertThrows(
        InvalidRunTransitionException.class,
        () -> RunStatus.COMPLETED.transitionTo(RunStatus.RUNNING));
  }

  @Test
  void runTransitionIncrementsVersionAndUpdatesTimestamp() {
    Instant createdAt = Instant.parse("2026-07-27T03:00:00Z");
    Instant updatedAt = Instant.parse("2026-07-27T03:00:01Z");
    Run run =
        new Run(
            new RunId("run_1"),
            new ConversationId("conv_1"),
            new TenantId("tenant_1"),
            "sample-agent",
            RunStatus.ACCEPTED,
            null,
            0,
            createdAt,
            createdAt);

    Run next = run.transitionTo(RunStatus.STARTING, updatedAt);

    assertEquals(RunStatus.STARTING, next.status());
    assertEquals(1, next.version());
    assertEquals(updatedAt, next.updatedAt());
  }
}
