package com.usig.application.usecase;

import com.usig.application.memory.ServiceRegistryEntry;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ServiceRegistryReloader;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.service.ServiceDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ReloadServiceRegistryUseCase {

  private final ServiceRegistryReloader reloader;
  private final Map<String, DownstreamExecutionPort> adaptersById;
  private final ServiceRegistryReloadPolicy policy;

  public ReloadServiceRegistryUseCase(
      ServiceRegistryReloader reloader,
      Map<String, DownstreamExecutionPort> adaptersById,
      ServiceRegistryReloadPolicy policy) {
    this.reloader = Objects.requireNonNull(reloader, "reloader must not be null");
    this.adaptersById =
        Map.copyOf(Objects.requireNonNull(adaptersById, "adaptersById must not be null"));
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public List<ServiceDescriptor> reload(String adminToken, List<ServiceDescriptor> services) {
    if (!policy.enabled()) {
      throw new UseCaseException(ErrorCode.FORBIDDEN, "service registry reload is disabled");
    }
    if (!policy.adminToken().equals(adminToken)) {
      throw new UseCaseException(ErrorCode.UNAUTHORIZED, "invalid service registry reload token");
    }
    List<ServiceRegistryEntry> entries =
        Objects.requireNonNullElse(services, List.<ServiceDescriptor>of()).stream()
            .map(service -> new ServiceRegistryEntry(service, adapter(service)))
            .toList();
    reloader.reload(entries);
    return entries.stream().map(ServiceRegistryEntry::service).toList();
  }

  private DownstreamExecutionPort adapter(ServiceDescriptor service) {
    DownstreamExecutionPort adapter = adaptersById.get(service.adapterId());
    if (adapter == null) {
      throw new UseCaseException(
          ErrorCode.ADAPTER_NOT_FOUND, "adapter not found: " + service.adapterId());
    }
    return adapter;
  }
}
