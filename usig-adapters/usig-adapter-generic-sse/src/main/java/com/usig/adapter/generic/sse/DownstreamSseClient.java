package com.usig.adapter.generic.sse;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceDescriptor;
import java.util.concurrent.CompletionStage;

public interface DownstreamSseClient {

  CompletionStage<DownstreamSseStream> open(
      CreateRunCommand request,
      ExecutionContext context,
      ServiceDescriptor service,
      GenericSseAdapterConfig config);

  CompletionStage<Void> cancel(ExecutionHandle handle, ExecutionContext context);
}
