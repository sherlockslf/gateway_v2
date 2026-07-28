package com.usig.adapter.openai;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceDescriptor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface OpenAiCompatibleClient {

  CompletionStage<OpenAiCompatibleStream> open(
      CreateRunCommand request,
      OpenAiCompatibleRequest downstreamRequest,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config);

  default CompletionStage<Void> cancel(ExecutionHandle handle, ExecutionContext context) {
    return CompletableFuture.completedFuture(null);
  }

  default CompletionStage<Void> cancel(
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    return cancel(handle, context);
  }

  default CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    return CompletableFuture.completedFuture(DownstreamExecutionState.UNKNOWN);
  }

  default CompletionStage<Void> submitInput(
      SubmitInputCommand command,
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    return CompletableFuture.failedFuture(
        new OpenAiCompatibleAdapterException(
            "OpenAI-compatible adapter does not support human input"));
  }

  default CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle,
      String cursor,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    return CompletableFuture.failedFuture(
        new OpenAiCompatibleAdapterException("OpenAI-compatible adapter does not support resume"));
  }
}
