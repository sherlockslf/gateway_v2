package com.usig.adapter.spi;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceDescriptor;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface StreamingServiceAdapter extends DownstreamExecutionPort {

  String adapterId();

  boolean supports(ServiceDescriptor service);

  @Override
  CompletionStage<AdapterExecution> execute(CreateRunCommand request, ExecutionContext context);

  @Override
  CompletionStage<DownstreamExecutionState> cancel(
      ExecutionHandle handle, ExecutionContext context);

  @Override
  CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context);

  @Override
  CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle, String cursor, ExecutionContext context);

  @Override
  CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context);
}
