package com.usig.application.memory;

import com.usig.application.port.DownstreamExecutionPort;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Objects;

public record ServiceRegistryEntry(ServiceDescriptor service, DownstreamExecutionPort adapter) {

  public ServiceRegistryEntry {
    service = Objects.requireNonNull(service, "service must not be null");
    adapter = Objects.requireNonNull(adapter, "adapter must not be null");
  }
}
