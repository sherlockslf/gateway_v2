package com.usig.application.port;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface DownstreamExecutionPort {

  CompletionStage<AdapterExecution> execute(CreateRunCommand command, ExecutionContext context);

  CompletionStage<DownstreamExecutionState> cancel(
      ExecutionHandle handle, ExecutionContext context);

  CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context);

  CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle, String cursor, ExecutionContext context);

  CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context);

  default CompletionStage<Void> submitInput(
      SubmitInputCommand command, ExecutionHandle handle, ExecutionContext context) {
    return submitInput(command, context);
  }
}
