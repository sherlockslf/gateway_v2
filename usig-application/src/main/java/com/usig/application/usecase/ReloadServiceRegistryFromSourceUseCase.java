package com.usig.application.usecase;

import com.usig.application.memory.ServiceRegistryEntry;
import com.usig.application.port.ServiceRegistryReloader;
import com.usig.application.port.ServiceRegistrySnapshotSource;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class ReloadServiceRegistryFromSourceUseCase {

  private final ServiceRegistrySnapshotSource source;
  private final ServiceRegistryReloader reloader;

  public ReloadServiceRegistryFromSourceUseCase(
      ServiceRegistrySnapshotSource source, ServiceRegistryReloader reloader) {
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.reloader = Objects.requireNonNull(reloader, "reloader must not be null");
  }

  public CompletionStage<ServiceRegistrySourceReloadResult> reload() {
    return source
        .load()
        .thenApply(
            entries -> {
              List<ServiceRegistryEntry> snapshot = List.copyOf(entries);
              reloader.reload(snapshot);
              return new ServiceRegistrySourceReloadResult(snapshot.size());
            });
  }
}
