package com.usig.application.port;

import java.time.Instant;

public interface ClockPort {

  Instant now();
}
