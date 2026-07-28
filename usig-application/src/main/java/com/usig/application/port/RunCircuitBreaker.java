package com.usig.application.port;

import com.usig.domain.service.ServiceDescriptor;

public interface RunCircuitBreaker {

  void beforeCall(ServiceDescriptor service);

  void onSuccess(ServiceDescriptor service);

  void onFailure(ServiceDescriptor service, Throwable throwable);
}
