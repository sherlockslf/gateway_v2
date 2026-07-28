package com.usig.adapter.agent.a2a;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.run.ExecutionHandle;
import java.util.concurrent.CompletionStage;

public interface AgentA2aClient {

  CompletionStage<AgentA2aStream> open(
      CreateRunCommand request, ExecutionContext context, AgentA2aAdapterConfig config);

  CompletionStage<Void> cancel(
      ExecutionHandle handle, ExecutionContext context, AgentA2aAdapterConfig config);

  CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context, AgentA2aAdapterConfig config);

  CompletionStage<AgentA2aStream> resume(
      ExecutionHandle handle,
      String cursor,
      ExecutionContext context,
      AgentA2aAdapterConfig config);

  CompletionStage<Void> submitInput(
      SubmitInputCommand command, ExecutionContext context, AgentA2aAdapterConfig config);
}
