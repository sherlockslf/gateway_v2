package com.usig.application.memory;

import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;

public final class NoopRunConcurrencyLimiter implements RunConcurrencyLimiter {

  private static final Permit NOOP_PERMIT = () -> {};

  @Override
  public Permit acquire(TenantId tenantId, ServiceDescriptor service) {
    return NOOP_PERMIT;
  }
}
