package com.usig.application.memory;

import com.usig.application.port.RunIdGenerator;
import com.usig.domain.run.RunId;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemoryRunIdGenerator implements RunIdGenerator {

  private final AtomicLong sequence = new AtomicLong();

  @Override
  public RunId nextRunId() {
    return new RunId("run_" + sequence.incrementAndGet());
  }
}
