package com.usig.domain.service;

import java.net.URI;
import java.util.Objects;
import java.util.Set;

public record ServiceDescriptor(
    String serviceId,
    ServiceType serviceType,
    URI endpoint,
    String adapterId,
    Set<String> visibleTenantIds,
    Set<Transport> transports,
    ServiceCapabilities capabilities,
    String serviceGroup,
    ServiceLimits limits,
    ServiceCircuitBreakerPolicy circuitBreakerPolicy,
    Set<String> allowedImageHosts,
    long version) {

  public ServiceDescriptor(
      String serviceId,
      ServiceType serviceType,
      URI endpoint,
      String adapterId,
      Set<String> visibleTenantIds,
      Set<Transport> transports,
      ServiceCapabilities capabilities,
      long version) {
    this(
        serviceId,
        serviceType,
        endpoint,
        adapterId,
        visibleTenantIds,
        transports,
        capabilities,
        null,
        ServiceLimits.defaults(),
        ServiceCircuitBreakerPolicy.defaults(),
        Set.of(),
        version);
  }

  public ServiceDescriptor(
      String serviceId,
      ServiceType serviceType,
      URI endpoint,
      String adapterId,
      Set<String> visibleTenantIds,
      Set<Transport> transports,
      ServiceCapabilities capabilities,
      ServiceLimits limits,
      long version) {
    this(
        serviceId,
        serviceType,
        endpoint,
        adapterId,
        visibleTenantIds,
        transports,
        capabilities,
        null,
        limits,
        ServiceCircuitBreakerPolicy.defaults(),
        Set.of(),
        version);
  }

  public ServiceDescriptor(
      String serviceId,
      ServiceType serviceType,
      URI endpoint,
      String adapterId,
      Set<String> visibleTenantIds,
      Set<Transport> transports,
      ServiceCapabilities capabilities,
      String serviceGroup,
      ServiceLimits limits,
      long version) {
    this(
        serviceId,
        serviceType,
        endpoint,
        adapterId,
        visibleTenantIds,
        transports,
        capabilities,
        serviceGroup,
        limits,
        ServiceCircuitBreakerPolicy.defaults(),
        Set.of(),
        version);
  }

  public ServiceDescriptor(
      String serviceId,
      ServiceType serviceType,
      URI endpoint,
      String adapterId,
      Set<String> visibleTenantIds,
      Set<Transport> transports,
      ServiceCapabilities capabilities,
      String serviceGroup,
      ServiceLimits limits,
      ServiceCircuitBreakerPolicy circuitBreakerPolicy,
      long version) {
    this(
        serviceId,
        serviceType,
        endpoint,
        adapterId,
        visibleTenantIds,
        transports,
        capabilities,
        serviceGroup,
        limits,
        circuitBreakerPolicy,
        Set.of(),
        version);
  }

  public ServiceDescriptor {
    serviceId = requireText(serviceId, "serviceId");
    serviceType = Objects.requireNonNull(serviceType, "serviceType must not be null");
    endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
    adapterId = requireText(adapterId, "adapterId");
    visibleTenantIds = Set.copyOf(Objects.requireNonNullElse(visibleTenantIds, Set.of()));
    transports = Set.copyOf(Objects.requireNonNullElse(transports, Set.of()));
    capabilities = Objects.requireNonNull(capabilities, "capabilities must not be null");
    serviceGroup = normalizeOptionalText(serviceGroup);
    limits = Objects.requireNonNullElse(limits, ServiceLimits.defaults());
    circuitBreakerPolicy =
        Objects.requireNonNullElse(circuitBreakerPolicy, ServiceCircuitBreakerPolicy.defaults());
    Set<String> configuredAllowedImageHosts =
        allowedImageHosts == null ? Set.of() : allowedImageHosts;
    allowedImageHosts =
        Set.copyOf(
            configuredAllowedImageHosts.stream()
                .map(ServiceDescriptor::normalizeRequiredLowercaseText)
                .collect(java.util.stream.Collectors.toUnmodifiableSet()));
    if (version < 1) {
      throw new IllegalArgumentException("version must be positive");
    }
  }

  public boolean supportsTransport(Transport transport) {
    return transports.contains(transport);
  }

  public boolean visibleToTenant(String tenantId) {
    return visibleTenantIds.isEmpty() || visibleTenantIds.contains(tenantId);
  }

  public String isolationGroupKey() {
    return serviceGroup == null ? serviceId : serviceGroup;
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

  private static String normalizeRequiredLowercaseText(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("allowedImageHosts must not contain blank host");
    }
    return value.trim().toLowerCase(java.util.Locale.ROOT);
  }
}
