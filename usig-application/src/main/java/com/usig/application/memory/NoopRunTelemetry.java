package com.usig.application.memory;

import com.usig.application.port.RunTelemetry;
import com.usig.domain.run.Run;
import java.time.Duration;

public final class NoopRunTelemetry implements RunTelemetry {

  @Override
  public void runCreated(Run run) {}

  @Override
  public void runCreationReused(Run run) {}

  @Override
  public void runCreateRetryScheduled(Run run, int failedAttempt, Duration delay) {}
}
