package com.usig.bootstrap;

import com.usig.adapter.spi.StreamingServiceAdapter;
import com.usig.application.memory.InMemoryServiceRegistry;
import com.usig.application.port.ServiceRegistryAuditPublisher;
import com.usig.domain.service.RetrySafety;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceCircuitBreakerPolicy;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceLimits;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.Config;

final class ConfiguredServiceRegistryFactory {

  private static final String PREFIX = "usig.services.";

  private ConfiguredServiceRegistryFactory() {}

  static InMemoryServiceRegistry create(
      Config config, Map<String, StreamingServiceAdapter> adaptersById) {
    return create(
        config,
        adaptersById,
        event -> java.util.concurrent.CompletableFuture.completedFuture(null),
        Instant::now);
  }

  static InMemoryServiceRegistry create(
      Config config,
      Map<String, StreamingServiceAdapter> adaptersById,
      ServiceRegistryAuditPublisher auditPublisher,
      Supplier<Instant> clock) {
    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(adaptersById, "adaptersById must not be null");
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry(auditPublisher, clock);
    for (String serviceId : csv(config, "usig.services.ids", Set.of("sample-agent"))) {
      ServiceDescriptor descriptor = descriptor(config, serviceId);
      StreamingServiceAdapter adapter = adaptersById.get(descriptor.adapterId());
      if (adapter == null) {
        throw new IllegalStateException("adapter not configured: " + descriptor.adapterId());
      }
      if (!adapter.supports(descriptor)) {
        throw new IllegalStateException(
            "adapter does not support service: "
                + descriptor.adapterId()
                + " -> "
                + descriptor.serviceId());
      }
      registry.register(descriptor, adapter);
    }
    return registry;
  }

  private static ServiceDescriptor descriptor(Config config, String serviceId) {
    String servicePrefix = PREFIX + serviceId + ".";
    return new ServiceDescriptor(
        serviceId,
        enumValue(config, servicePrefix + "service-type", ServiceType.class, ServiceType.AGENT),
        URI.create(
            text(config, servicePrefix + "endpoint", "https://internal.example/" + serviceId)),
        text(config, servicePrefix + "adapter-id", "sample-agent-v1"),
        csv(config, servicePrefix + "visible-tenants", Set.of()),
        enumCsv(config, servicePrefix + "transports", Transport.class, Set.of(Transport.SSE)),
        capabilities(config, servicePrefix + "capabilities."),
        optionalText(config, servicePrefix + "service-group"),
        limits(config, servicePrefix + "limits."),
        circuitBreakerPolicy(config, servicePrefix + "circuit-breaker."),
        csv(config, servicePrefix + "allowed-image-hosts", Set.of()),
        integer(config, servicePrefix + "version", 1));
  }

  private static ServiceCapabilities capabilities(Config config, String prefix) {
    return new ServiceCapabilities(
        bool(config, prefix + "streaming", true),
        bool(config, prefix + "cancel", true),
        bool(config, prefix + "resume", false),
        bool(config, prefix + "replay", false),
        bool(config, prefix + "task-query", true),
        bool(config, prefix + "human-input", false),
        bool(config, prefix + "multimodal-input", false),
        bool(config, prefix + "structured-output", true),
        enumValue(config, prefix + "retry-safety", RetrySafety.class, RetrySafety.NONE));
  }

  private static ServiceLimits limits(Config config, String prefix) {
    return new ServiceLimits(
        integer(
            config, prefix + "max-concurrent-runs", ServiceLimits.defaults().maxConcurrentRuns()),
        integer(
            config,
            prefix + "max-concurrent-runs-per-tenant",
            ServiceLimits.defaults().maxConcurrentRunsPerTenant()),
        integer(
            config,
            prefix + "max-concurrent-runs-per-group",
            ServiceLimits.defaults().maxConcurrentRunsPerGroup()));
  }

  private static ServiceCircuitBreakerPolicy circuitBreakerPolicy(Config config, String prefix) {
    return new ServiceCircuitBreakerPolicy(
        integer(
            config,
            prefix + "failure-threshold",
            ServiceCircuitBreakerPolicy.defaults().failureThreshold()),
        duration(
            config,
            prefix + "open-duration",
            ServiceCircuitBreakerPolicy.defaults().openDuration()),
        integer(
            config,
            prefix + "half-open-max-probe-runs",
            ServiceCircuitBreakerPolicy.defaults().halfOpenMaxProbeRuns()));
  }

  private static boolean bool(Config config, String key, boolean defaultValue) {
    return config.getOptionalValue(key, Boolean.class).orElse(defaultValue);
  }

  private static int integer(Config config, String key, int defaultValue) {
    return config.getOptionalValue(key, Integer.class).orElse(defaultValue);
  }

  private static Duration duration(Config config, String key, Duration defaultValue) {
    return config.getOptionalValue(key, Duration.class).orElse(defaultValue);
  }

  private static String text(Config config, String key, String defaultValue) {
    return config
        .getOptionalValue(key, String.class)
        .filter(value -> !value.isBlank())
        .orElse(defaultValue);
  }

  private static String optionalText(Config config, String key) {
    return config
        .getOptionalValue(key, String.class)
        .filter(value -> !value.isBlank())
        .orElse(null);
  }

  private static Set<String> csv(Config config, String key, Set<String> defaultValue) {
    return config
        .getOptionalValue(key, String.class)
        .map(ConfiguredServiceRegistryFactory::splitCsv)
        .orElse(defaultValue);
  }

  private static <E extends Enum<E>> Set<E> enumCsv(
      Config config, String key, Class<E> enumType, Set<E> defaultValue) {
    return config
        .getOptionalValue(key, String.class)
        .map(
            values ->
                splitCsv(values).stream()
                    .map(value -> Enum.valueOf(enumType, value))
                    .collect(Collectors.toUnmodifiableSet()))
        .orElse(defaultValue);
  }

  private static <E extends Enum<E>> E enumValue(
      Config config, String key, Class<E> enumType, E defaultValue) {
    return config
        .getOptionalValue(key, String.class)
        .map(value -> Enum.valueOf(enumType, value))
        .orElse(defaultValue);
  }

  private static Set<String> splitCsv(String values) {
    return Arrays.stream(values.split(","))
        .map(String::trim)
        .filter(value -> !value.isBlank())
        .collect(Collectors.toUnmodifiableSet());
  }
}
