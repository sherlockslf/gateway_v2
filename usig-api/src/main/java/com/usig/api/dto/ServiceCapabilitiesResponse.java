package com.usig.api.dto;

import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Set;
import java.util.stream.Collectors;

public record ServiceCapabilitiesResponse(
    String serviceId,
    String serviceType,
    String adapterId,
    Set<String> transports,
    Capabilities capabilities,
    long version) {

  public static ServiceCapabilitiesResponse from(ServiceDescriptor service) {
    return new ServiceCapabilitiesResponse(
        service.serviceId(),
        service.serviceType().name(),
        service.adapterId(),
        service.transports().stream().map(Enum::name).collect(Collectors.toUnmodifiableSet()),
        Capabilities.from(service.capabilities()),
        service.version());
  }

  public record Capabilities(
      boolean streaming,
      boolean cancel,
      boolean resume,
      boolean replay,
      boolean taskQuery,
      boolean humanInput,
      boolean multimodalInput,
      boolean structuredOutput,
      String retrySafety) {

    private static Capabilities from(ServiceCapabilities capabilities) {
      return new Capabilities(
          capabilities.streaming(),
          capabilities.cancel(),
          capabilities.resume(),
          capabilities.replay(),
          capabilities.taskQuery(),
          capabilities.humanInput(),
          capabilities.multimodalInput(),
          capabilities.structuredOutput(),
          capabilities.retrySafety().name());
    }
  }
}
