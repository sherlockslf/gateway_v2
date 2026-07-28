package com.usig.application.port;

import com.usig.domain.run.Run;
import java.time.Duration;

public interface RunTelemetry {

  void runCreated(Run run);

  void runCreationReused(Run run);

  void runCreateRetryScheduled(Run run, int failedAttempt, Duration delay);
}
