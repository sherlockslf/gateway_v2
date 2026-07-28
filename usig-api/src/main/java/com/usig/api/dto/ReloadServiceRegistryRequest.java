package com.usig.api.dto;

import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public record ReloadServiceRegistryRequest(List<Service> services) {

  public List<ServiceDescriptor> toServices() {
    return Objects.requireNonNullElse(services, List.<Service>of()).stream()
        .map(Service::toDescriptor)
        .toList();
  }

  public record Service(
      String serviceId,
      String serviceType,
      String endpoint,
      String adapterId,
      Set<String> visibleTenantIds,
      Set<String> transports,
      Capabilities capabilities,
      String serviceGroup,
      Limits limits,
      CircuitBreakerPolicy circuitBreakerPolicy,
      Set<String> allowedImageHosts,
      Long version) {

    private ServiceDescriptor toDescriptor() {
      return new ServiceDescriptor(
          serviceId,
          enumValue(serviceType, ServiceType.class, ServiceType.AGENT),
          URI.create(endpoint),
          adapterId,
          Objects.requireNonNullElse(visibleTenantIds, Set.of()),
          enumSet(transports, Transport.class, Set.of(Transport.SSE)),
          Objects.requireNonNullElse(capabilities, Capabilities.defaults()).toDomain(),
          serviceGroup,
          Objects.requireNonNullElse(limits, Limits.defaults()).toDomain(),
          Objects.requireNonNullElse(circuitBreakerPolicy, CircuitBreakerPolicy.defaults())
              .toDomain(),
          Objects.requireNonNullElse(allowedImageHosts, Set.of()),
          Objects.requireNonNullElse(version, 1L));
    }
  }

  public record Capabilities(
      Boolean streaming,
      Boolean cancel,
      Boolean resume,
      Boolean replay,
      Boolean taskQuery,
      Boolean humanInput,
      Boolean multimodalInput,
      Boolean structuredOutput,
      String retrySafety) {

    private static Capabilities defaults() {
      return new Capabilities(true, true, false, false, true, false, false, true, "NONE");
    }

    private ServiceCapabilities toDomain() {
      return new ServiceCapabilities(
          Objects.requireNonNullElse(streaming, true),
          Objects.requireNonNullElse(cancel, true),
          Objects.requireNonNullElse(resume, false),
          Objects.requireNonNullElse(replay, false),
          Objects.requireNonNullElse(taskQuery, true),
          Objects.requireNonNullElse(humanInput, false),
          Objects.requireNonNullElse(multimodalInput, false),
          Objects.requireNonNullElse(structuredOutput, true),
          enumValue(retrySafety, RetrySafety.class, RetrySafety.NONE));
    }
  }

  public record Limits(
      Integer maxConcurrentRuns,
      Integer maxConcurrentRunsPerTenant,
      Integer maxConcurrentRunsPerGroup) {

    private static Limits defaults() {
      ServiceLimits defaults = ServiceLimits.defaults();
      return new Limits(
          defaults.maxConcurrentRuns(),
          defaults.maxConcurrentRunsPerTenant(),
          defaults.maxConcurrentRunsPerGroup());
    }

    private ServiceLimits toDomain() {
      ServiceLimits defaults = ServiceLimits.defaults();
      return new ServiceLimits(
          Objects.requireNonNullElse(maxConcurrentRuns, defaults.maxConcurrentRuns()),
          Objects.requireNonNullElse(
              maxConcurrentRunsPerTenant, defaults.maxConcurrentRunsPerTenant()),
          Objects.requireNonNullElse(
              maxConcurrentRunsPerGroup, defaults.maxConcurrentRunsPerGroup()));
    }
  }

  public record CircuitBreakerPolicy(
      Integer failureThreshold, String openDuration, Integer halfOpenMaxProbeRuns) {

    private static CircuitBreakerPolicy defaults() {
      ServiceCircuitBreakerPolicy defaults = ServiceCircuitBreakerPolicy.defaults();
      return new CircuitBreakerPolicy(
          defaults.failureThreshold(),
          defaults.openDuration().toString(),
          defaults.halfOpenMaxProbeRuns());
    }

    private ServiceCircuitBreakerPolicy toDomain() {
      ServiceCircuitBreakerPolicy defaults = ServiceCircuitBreakerPolicy.defaults();
      return new ServiceCircuitBreakerPolicy(
          Objects.requireNonNullElse(failureThreshold, defaults.failureThreshold()),
          openDuration == null ? defaults.openDuration() : Duration.parse(openDuration),
          Objects.requireNonNullElse(halfOpenMaxProbeRuns, defaults.halfOpenMaxProbeRuns()));
    }
  }

  private static <E extends Enum<E>> E enumValue(String value, Class<E> enumType, E defaultValue) {
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return Enum.valueOf(enumType, value);
  }

  private static <E extends Enum<E>> Set<E> enumSet(
      Set<String> values, Class<E> enumType, Set<E> defaultValue) {
    if (values == null) {
      return defaultValue;
    }
    return values.stream().map(value -> Enum.valueOf(enumType, value)).collect(Collectors.toSet());
  }
}
