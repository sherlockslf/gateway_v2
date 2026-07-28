package com.usig.application.usecase;

import java.util.Objects;

public record ServiceRegistryReloadPolicy(boolean enabled, String adminToken) {

  public ServiceRegistryReloadPolicy {
    adminToken = Objects.requireNonNullElse(adminToken, "");
    if (enabled && adminToken.isBlank()) {
      throw new IllegalArgumentException("adminToken must be configured when reload is enabled");
    }
  }

  public static ServiceRegistryReloadPolicy disabled() {
    return new ServiceRegistryReloadPolicy(false, "");
  }
}
