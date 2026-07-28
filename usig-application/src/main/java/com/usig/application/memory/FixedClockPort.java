package com.usig.application.memory;

import com.usig.application.port.ClockPort;
import java.time.Instant;

public final class FixedClockPort implements ClockPort {

  private final Instant now;

  public FixedClockPort(Instant now) {
    this.now = now;
  }

  @Override
  public Instant now() {
    return now;
  }
}
