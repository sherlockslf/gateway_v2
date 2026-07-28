package com.usig.application.port;

import com.usig.application.memory.ServiceRegistryEntry;
import java.util.Collection;

public interface ServiceRegistryReloader {

  void reload(Collection<ServiceRegistryEntry> entries);
}
