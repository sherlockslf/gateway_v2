package com.usig.domain.run;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum RunStatus {
  ACCEPTED,
  STARTING,
  RUNNING,
  WAITING_INPUT,
  CANCEL_REQUESTED,
  COMPLETED,
  FAILED,
  CANCELED,
  UNKNOWN;

  private static final Map<RunStatus, Set<RunStatus>> ALLOWED_TRANSITIONS =
      Map.of(
          ACCEPTED, EnumSet.of(STARTING, FAILED),
          STARTING, EnumSet.of(RUNNING, FAILED, CANCEL_REQUESTED),
          RUNNING, EnumSet.of(WAITING_INPUT, COMPLETED, FAILED, CANCEL_REQUESTED),
          WAITING_INPUT, EnumSet.of(RUNNING, CANCEL_REQUESTED, FAILED),
          CANCEL_REQUESTED, EnumSet.of(CANCELED, COMPLETED, FAILED),
          COMPLETED, EnumSet.noneOf(RunStatus.class),
          FAILED, EnumSet.noneOf(RunStatus.class),
          CANCELED, EnumSet.noneOf(RunStatus.class),
          UNKNOWN, EnumSet.of(FAILED));

  public boolean canTransitionTo(RunStatus next) {
    return ALLOWED_TRANSITIONS.get(this).contains(next);
  }

  public RunStatus transitionTo(RunStatus next) {
    if (!canTransitionTo(next)) {
      throw new InvalidRunTransitionException(this, next);
    }
    return next;
  }

  public boolean terminal() {
    return this == COMPLETED || this == FAILED || this == CANCELED;
  }
}
