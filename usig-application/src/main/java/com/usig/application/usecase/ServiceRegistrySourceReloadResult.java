package com.usig.application.usecase;

public record ServiceRegistrySourceReloadResult(int serviceCount) {

  public ServiceRegistrySourceReloadResult {
    if (serviceCount < 0) {
      throw new IllegalArgumentException("serviceCount must not be negative");
    }
  }
}
