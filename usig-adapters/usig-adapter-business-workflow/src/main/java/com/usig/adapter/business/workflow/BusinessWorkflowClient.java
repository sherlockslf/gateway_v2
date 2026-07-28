package com.usig.adapter.business.workflow;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.run.ExecutionHandle;
import java.util.concurrent.CompletionStage;

public interface BusinessWorkflowClient {

  CompletionStage<BusinessWorkflowStream> open(
      CreateRunCommand request, ExecutionContext context, BusinessWorkflowAdapterConfig config);

  CompletionStage<Void> cancel(
      ExecutionHandle handle, ExecutionContext context, BusinessWorkflowAdapterConfig config);

  CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context, BusinessWorkflowAdapterConfig config);

  CompletionStage<BusinessWorkflowStream> resume(
      ExecutionHandle handle,
      String cursor,
      ExecutionContext context,
      BusinessWorkflowAdapterConfig config);

  CompletionStage<Void> submitInput(
      SubmitInputCommand command, ExecutionContext context, BusinessWorkflowAdapterConfig config);
}
