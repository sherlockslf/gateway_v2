package com.usig.bootstrap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.usig.application.memory.InMemoryRunCircuitBreaker;
import com.usig.application.port.RunCircuitBreaker;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class CircuitBreakerBackendTest {

  @Inject RunCircuitBreaker circuitBreaker;

  @Test
  void defaultsToInMemoryCircuitBreaker() {
    assertInstanceOf(InMemoryRunCircuitBreaker.class, circuitBreaker);
  }
}
