package com.usig.application.port;

import com.usig.domain.service.ServiceDescriptor;
import java.util.List;
import java.util.Optional;

public interface ServiceRegistry {

  Optional<ServiceDescriptor> find(String serviceId);

  List<ServiceDescriptor> list();

  DownstreamExecutionPort adapterFor(ServiceDescriptor service);
}
