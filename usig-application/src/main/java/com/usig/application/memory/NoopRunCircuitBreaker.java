package com.usig.application.memory;

import com.usig.application.port.RunCircuitBreaker;
import com.usig.domain.service.ServiceDescriptor;

public final class NoopRunCircuitBreaker implements RunCircuitBreaker {

  @Override
  public void beforeCall(ServiceDescriptor service) {}

  @Override
  public void onSuccess(ServiceDescriptor service) {}

  @Override
  public void onFailure(ServiceDescriptor service, Throwable throwable) {}
}
