package com.usig.application.port;

import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;

public interface RunConcurrencyLimiter {

  Permit acquire(TenantId tenantId, ServiceDescriptor service);

  interface Permit extends AutoCloseable {

    @Override
    void close();
  }
}
