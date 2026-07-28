package com.usig.api.dto;

import com.usig.domain.service.ServiceDescriptor;
import java.util.List;

public record ReloadServiceRegistryResponse(
    int serviceCount, List<ServiceCapabilitiesResponse> services) {

  public static ReloadServiceRegistryResponse from(List<ServiceDescriptor> services) {
    List<ServiceCapabilitiesResponse> responses =
        services.stream().map(ServiceCapabilitiesResponse::from).toList();
    return new ReloadServiceRegistryResponse(responses.size(), responses);
  }
}
