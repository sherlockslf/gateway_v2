package com.usig.infra.postgres;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface PostgresServiceRegistryCommands {

  CompletionStage<Optional<PostgresServiceDescriptorRecord>> findByServiceId(String serviceId);

  CompletionStage<List<PostgresServiceDescriptorRecord>> list();
}
