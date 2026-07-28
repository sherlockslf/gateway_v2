package com.usig.application.memory;

import com.usig.application.port.ClockPort;
import com.usig.application.port.RunCircuitBreaker;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class InMemoryRunCircuitBreaker implements RunCircuitBreaker {

  private final Object lock = new Object();
  private final ClockPort clock;
  private final Map<String, State> states = new HashMap<>();

  public InMemoryRunCircuitBreaker(ClockPort clock) {
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public void beforeCall(ServiceDescriptor service) {
    Objects.requireNonNull(service, "service must not be null");
    synchronized (lock) {
      State state = states.get(service.isolationGroupKey());
      if (state != null && state.openUntil() != null) {
        if (clock.now().isBefore(state.openUntil())) {
          throw new UseCaseException(ErrorCode.DOWNSTREAM_CIRCUIT_OPEN);
        }
        if (state.halfOpenProbeRuns() >= service.circuitBreakerPolicy().halfOpenMaxProbeRuns()) {
          throw new UseCaseException(ErrorCode.DOWNSTREAM_CIRCUIT_OPEN);
        }
        states.put(
            service.isolationGroupKey(),
            new State(state.failures(), state.openUntil(), state.halfOpenProbeRuns() + 1));
      }
    }
  }

  @Override
  public void onSuccess(ServiceDescriptor service) {
    Objects.requireNonNull(service, "service must not be null");
    synchronized (lock) {
      states.remove(service.isolationGroupKey());
    }
  }

  @Override
  public void onFailure(ServiceDescriptor service, Throwable throwable) {
    Objects.requireNonNull(service, "service must not be null");
    synchronized (lock) {
      String groupKey = service.isolationGroupKey();
      State current = states.getOrDefault(groupKey, new State(0, null, 0));
      int failures = current.failures() + 1;
      Instant openUntil =
          failures >= service.circuitBreakerPolicy().failureThreshold()
              ? clock.now().plus(service.circuitBreakerPolicy().openDuration())
              : null;
      states.put(groupKey, new State(failures, openUntil, 0));
    }
  }

  private record State(int failures, Instant openUntil, int halfOpenProbeRuns) {}
}
