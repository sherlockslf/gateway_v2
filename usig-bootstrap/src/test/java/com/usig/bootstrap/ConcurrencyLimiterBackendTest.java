package com.usig.bootstrap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.usig.application.memory.InMemoryRunConcurrencyLimiter;
import com.usig.application.port.RunConcurrencyLimiter;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ConcurrencyLimiterBackendTest {

  @Inject RunConcurrencyLimiter limiter;

  @Test
  void defaultsToInMemoryConcurrencyLimiter() {
    assertInstanceOf(InMemoryRunConcurrencyLimiter.class, limiter);
  }
}
