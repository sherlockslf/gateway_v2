package com.usig.application.port;

import com.usig.application.memory.ServiceRegistryEntry;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface ServiceRegistrySnapshotSource {

  CompletionStage<List<ServiceRegistryEntry>> load();
}
