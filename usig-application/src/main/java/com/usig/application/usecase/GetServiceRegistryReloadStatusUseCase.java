package com.usig.application.usecase;

import com.usig.application.port.ServiceRegistryReloadStatus;
import com.usig.application.port.ServiceRegistryReloadStatusReader;
import com.usig.domain.error.ErrorCode;
import java.util.Objects;

public final class GetServiceRegistryReloadStatusUseCase {

  private final ServiceRegistryReloadStatusReader statusReader;
  private final ServiceRegistryReloadPolicy policy;

  public GetServiceRegistryReloadStatusUseCase(
      ServiceRegistryReloadStatusReader statusReader, ServiceRegistryReloadPolicy policy) {
    this.statusReader = Objects.requireNonNull(statusReader, "statusReader must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  public ServiceRegistryReloadStatus get(String adminToken) {
    if (!policy.enabled()) {
      throw new UseCaseException(ErrorCode.FORBIDDEN, "service registry reload is disabled");
    }
    if (!policy.adminToken().equals(adminToken)) {
      throw new UseCaseException(ErrorCode.UNAUTHORIZED, "invalid service registry reload token");
    }
    return statusReader.status();
  }
}
