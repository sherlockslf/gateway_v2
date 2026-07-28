package com.usig.infra.postgres;

import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public record PostgresServiceDescriptorRecord(
    String serviceId,
    String serviceType,
    String endpoint,
    String adapterId,
    Set<String> visibleTenantIds,
    Set<String> transports,
    Map<String, Object> capabilities,
    String serviceGroup,
    Map<String, Object> limits,
    Map<String, Object> circuitBreakerPolicy,
    Set<String> allowedImageHosts,
    long version) {

  public PostgresServiceDescriptorRecord {
    serviceId = requireText(serviceId, "serviceId");
    serviceType = requireText(serviceType, "serviceType");
    endpoint = requireText(endpoint, "endpoint");
    adapterId = requireText(adapterId, "adapterId");
    visibleTenantIds = Set.copyOf(Objects.requireNonNullElse(visibleTenantIds, Set.of()));
    transports = Set.copyOf(Objects.requireNonNullElse(transports, Set.of()));
    capabilities = Map.copyOf(Objects.requireNonNullElse(capabilities, Map.of()));
    serviceGroup = normalizeOptionalText(serviceGroup);
    limits = Map.copyOf(Objects.requireNonNullElse(limits, Map.of()));
    circuitBreakerPolicy = Map.copyOf(Objects.requireNonNullElse(circuitBreakerPolicy, Map.of()));
    allowedImageHosts = Set.copyOf(Objects.requireNonNullElse(allowedImageHosts, Set.of()));
    if (version < 1) {
      throw new IllegalArgumentException("version must be positive");
    }
  }

  public static PostgresServiceDescriptorRecord fromDomain(ServiceDescriptor service) {
    Objects.requireNonNull(service, "service must not be null");
    return new PostgresServiceDescriptorRecord(
        service.serviceId(),
        service.serviceType().name(),
        service.endpoint().toString(),
        service.adapterId(),
        service.visibleTenantIds(),
        service.transports().stream().map(Enum::name).collect(Collectors.toUnmodifiableSet()),
        capabilitiesMap(service.capabilities()),
        service.serviceGroup(),
        limitsMap(service.limits()),
        circuitBreakerPolicyMap(service.circuitBreakerPolicy()),
        service.allowedImageHosts(),
        service.version());
  }

  ServiceDescriptor toDomain() {
    return new ServiceDescriptor(
        serviceId,
        ServiceType.valueOf(serviceType),
        URI.create(endpoint),
        adapterId,
        visibleTenantIds,
        transports.stream()
            .map(value -> Transport.valueOf(requireText(value, "transport")))
            .collect(Collectors.toUnmodifiableSet()),
        toCapabilities(),
        serviceGroup,
        toLimits(),
        toCircuitBreakerPolicy(),
        allowedImageHosts,
        version);
  }

  private ServiceCapabilities toCapabilities() {
    return new ServiceCapabilities(
        bool(capabilities, "streaming", true),
        bool(capabilities, "cancel", false),
        bool(capabilities, "resume", false),
        bool(capabilities, "replay", false),
        bool(capabilities, "taskQuery", false),
        bool(capabilities, "humanInput", false),
        bool(capabilities, "multimodalInput", false),
        bool(capabilities, "structuredOutput", false),
        enumValue(capabilities, "retrySafety", RetrySafety.class, RetrySafety.NONE));
  }

  private ServiceLimits toLimits() {
    ServiceLimits defaults = ServiceLimits.defaults();
    return new ServiceLimits(
        integer(limits, "maxConcurrentRuns", defaults.maxConcurrentRuns()),
        integer(limits, "maxConcurrentRunsPerTenant", defaults.maxConcurrentRunsPerTenant()),
        integer(limits, "maxConcurrentRunsPerGroup", defaults.maxConcurrentRunsPerGroup()));
  }

  private ServiceCircuitBreakerPolicy toCircuitBreakerPolicy() {
    ServiceCircuitBreakerPolicy defaults = ServiceCircuitBreakerPolicy.defaults();
    return new ServiceCircuitBreakerPolicy(
        integer(circuitBreakerPolicy, "failureThreshold", defaults.failureThreshold()),
        duration(circuitBreakerPolicy, "openDuration", defaults.openDuration()),
        integer(circuitBreakerPolicy, "halfOpenMaxProbeRuns", defaults.halfOpenMaxProbeRuns()));
  }

  private static Map<String, Object> capabilitiesMap(ServiceCapabilities capabilities) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("streaming", capabilities.streaming());
    values.put("cancel", capabilities.cancel());
    values.put("resume", capabilities.resume());
    values.put("replay", capabilities.replay());
    values.put("taskQuery", capabilities.taskQuery());
    values.put("humanInput", capabilities.humanInput());
    values.put("multimodalInput", capabilities.multimodalInput());
    values.put("structuredOutput", capabilities.structuredOutput());
    values.put("retrySafety", capabilities.retrySafety().name());
    return Map.copyOf(values);
  }

  private static Map<String, Object> limitsMap(ServiceLimits limits) {
    return Map.of(
        "maxConcurrentRuns",
        limits.maxConcurrentRuns(),
        "maxConcurrentRunsPerTenant",
        limits.maxConcurrentRunsPerTenant(),
        "maxConcurrentRunsPerGroup",
        limits.maxConcurrentRunsPerGroup());
  }

  private static Map<String, Object> circuitBreakerPolicyMap(
      ServiceCircuitBreakerPolicy circuitBreakerPolicy) {
    return Map.of(
        "failureThreshold",
        circuitBreakerPolicy.failureThreshold(),
        "openDuration",
        circuitBreakerPolicy.openDuration().toString(),
        "halfOpenMaxProbeRuns",
        circuitBreakerPolicy.halfOpenMaxProbeRuns());
  }

  private static boolean bool(Map<String, Object> values, String key, boolean defaultValue) {
    Object value = values.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Boolean booleanValue) {
      return booleanValue;
    }
    if (value instanceof String stringValue) {
      return Boolean.parseBoolean(stringValue);
    }
    throw new IllegalArgumentException(key + " must be boolean");
  }

  private static int integer(Map<String, Object> values, String key, int defaultValue) {
    Object value = values.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number numberValue) {
      return numberValue.intValue();
    }
    if (value instanceof String stringValue) {
      return Integer.parseInt(stringValue);
    }
    throw new IllegalArgumentException(key + " must be integer");
  }

  private static Duration duration(Map<String, Object> values, String key, Duration defaultValue) {
    Object value = values.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Duration durationValue) {
      return durationValue;
    }
    if (value instanceof String stringValue) {
      return Duration.parse(stringValue);
    }
    throw new IllegalArgumentException(key + " must be duration");
  }

  private static <E extends Enum<E>> E enumValue(
      Map<String, Object> values, String key, Class<E> enumType, E defaultValue) {
    Object value = values.get(key);
    if (value == null) {
      return defaultValue;
    }
    return Enum.valueOf(enumType, value.toString());
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String normalizeOptionalText(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
