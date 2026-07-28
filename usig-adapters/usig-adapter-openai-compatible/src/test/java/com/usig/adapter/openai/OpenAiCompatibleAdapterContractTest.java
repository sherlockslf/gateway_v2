package com.usig.adapter.openai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class OpenAiCompatibleAdapterContractTest {

  private static final Duration DEFAULT_TOTAL_EXECUTION_TIMEOUT = Duration.ofHours(24);
  private static final Duration DEFAULT_CANCEL_TIMEOUT = Duration.ofSeconds(30);

  @Test
  void mapsTextUsageToolAndDoneChunksToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"id":"chatcmpl-1","choices":[{"index":0,"delta":{"content":"hello"}}]}

                data:{"id":"chatcmpl-1","choices":[{"index":0,"delta":{"tool_calls":[{"id":"call_1","function":{"name":"lookup","arguments":"{\\"city\\":\\"sh\\"}"}}]}}]}

                data:{"id":"chatcmpl-1","usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}

                data:[DONE]

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals("resp-1", execution.handle().downstreamRunId());
    assertEquals(4, events.size());
    assertEquals(EventType.TEXT_DELTA, events.get(0).eventType());
    assertEquals("hello", events.get(0).payload().get("content"));
    assertEquals(EventType.TOOL_ARGUMENTS_DELTA, events.get(1).eventType());
    assertEquals("{\"city\":\"sh\"}", events.get(1).payload().get("arguments"));
    assertEquals(EventType.USAGE_UPDATED, events.get(2).eventType());
    assertEquals(5, events.get(2).payload().get("total_tokens"));
    assertEquals(EventType.EXECUTION_COMPLETED, events.get(3).eventType());
    assertEquals(
        URI.create("https://models.internal.example/v1/chat/completions"),
        client.request.endpoint());
    assertEquals("Bearer token-1", client.request.headers().get("Authorization"));
    assertEquals("gpt-4o-mini", client.request.body().get("model"));
    assertEquals(true, client.request.body().get("stream"));
  }

  @Test
  void mapsTextAndImageInputToOpenAiCompatibleRequest() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    adapter.execute(multimodalCommand(), context()).toCompletableFuture().get();

    List<?> messages = (List<?>) client.request.body().get("messages");
    Map<?, ?> message = (Map<?, ?>) messages.getFirst();
    List<?> content = (List<?>) message.get("content");
    assertEquals("user", message.get("role"));
    assertEquals(Map.of("type", "text", "text", "describe"), content.getFirst());
    Map<?, ?> imagePart = (Map<?, ?>) content.get(1);
    assertEquals("image_url", imagePart.get("type"));
    assertEquals(
        Map.of("url", "https://objects.internal/img-1.png", "mimeType", "image/png"),
        imagePart.get("image_url"));
  }

  @Test
  void mapsTextAndImageInputToResponsesRequestWhenConfigured() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    OpenAiCompatibleAdapterConfig config =
        new OpenAiCompatibleAdapterConfig(
            service(URI.create("https://models.internal.example/v1/responses"), true),
            4096,
            Duration.ofSeconds(1),
            "gpt-4.1-mini",
            "Authorization",
            "token-1",
            OpenAiCompatibleApiMode.RESPONSES);
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config, client);

    adapter.execute(multimodalCommand(), context()).toCompletableFuture().get();

    assertEquals(
        URI.create("https://models.internal.example/v1/responses"), client.request.endpoint());
    assertTrue(!client.request.body().containsKey("messages"));
    List<?> input = (List<?>) client.request.body().get("input");
    Map<?, ?> message = (Map<?, ?>) input.getFirst();
    List<?> content = (List<?>) message.get("content");
    assertEquals("user", message.get("role"));
    assertEquals(Map.of("type", "input_text", "text", "describe"), content.getFirst());
    assertEquals(
        Map.of(
            "type",
            "input_image",
            "image_url",
            "https://objects.internal/img-1.png",
            "mime_type",
            "image/png"),
        content.get(1));
    assertTrue(!client.request.body().toString().contains("base64"));
  }

  @Test
  void injectsAuthorizationFromCredentialProvider() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    OpenAiCompatibleAdapterConfig config =
        new OpenAiCompatibleAdapterConfig(
            service(), 4096, Duration.ofSeconds(1), "gpt-4o-mini", "Authorization", null);
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config,
            client,
            new StaticOpenAiCompatibleCredentialProvider(Map.of("model-chat", "secret-token")));

    adapter.execute(command(), context()).toCompletableFuture().get();

    assertEquals("Bearer secret-token", client.request.headers().get("Authorization"));
  }

  @Test
  void omitsAuthorizationWhenCredentialProviderHasNoToken() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    OpenAiCompatibleAdapterConfig config =
        new OpenAiCompatibleAdapterConfig(
            service(), 4096, Duration.ofSeconds(1), "gpt-4o-mini", "Authorization", null);
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config, client, new StaticOpenAiCompatibleCredentialProvider(Map.of()));

    adapter.execute(command(), context()).toCompletableFuture().get();

    assertTrue(client.request.headers().containsKey("Accept"));
    assertTrue(!client.request.headers().containsKey("Authorization"));
  }

  @Test
  void readsAuthorizationFromSecretStoreByServiceIdWithoutPuttingTokenInBody() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    RecordingSecretStore secretStore = new RecordingSecretStore("secret-from-store");
    OpenAiCompatibleAdapterConfig config =
        new OpenAiCompatibleAdapterConfig(
            service(), 4096, Duration.ofSeconds(1), "gpt-4o-mini", "Authorization", null);
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config, client, new SecretStoreOpenAiCompatibleCredentialProvider(secretStore));

    adapter.execute(command(), context()).toCompletableFuture().get();

    assertEquals("model-chat", secretStore.serviceId);
    assertEquals("tenant_1", secretStore.tenantId);
    assertEquals("Bearer secret-from-store", client.request.headers().get("Authorization"));
    assertTrue(!client.request.body().toString().contains("secret-from-store"));
  }

  @Test
  void readsAuthorizationFromEnvironmentSecretStoreWithTenantOverride() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    EnvironmentOpenAiCompatibleSecretStore secretStore =
        new EnvironmentOpenAiCompatibleSecretStore(
            Map.of(
                    "USIG_OPENAI_TOKEN_TENANT_1_MODEL_CHAT", "tenant-token",
                    "USIG_OPENAI_TOKEN_MODEL_CHAT", "service-token")
                ::get);
    OpenAiCompatibleAdapterConfig config =
        new OpenAiCompatibleAdapterConfig(
            service(), 4096, Duration.ofSeconds(1), "gpt-4o-mini", "Authorization", null);
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config, client, new SecretStoreOpenAiCompatibleCredentialProvider(secretStore));

    adapter.execute(command(), context()).toCompletableFuture().get();

    assertEquals("Bearer tenant-token", client.request.headers().get("Authorization"));
    assertTrue(!client.request.body().toString().contains("tenant-token"));
    assertTrue(!client.request.body().toString().contains("service-token"));
  }

  @Test
  void environmentSecretStoreFallsBackToServiceTokenAndIgnoresBlankValues() {
    EnvironmentOpenAiCompatibleSecretStore secretStore =
        new EnvironmentOpenAiCompatibleSecretStore(
            key ->
                Map.of(
                        "USIG_OPENAI_TOKEN_TENANT_1_MODEL_CHAT", " ",
                        "USIG_OPENAI_TOKEN_MODEL_CHAT", "service-token")
                    .get(key));

    assertEquals("service-token", secretStore.bearerToken(service(), context()).orElseThrow());
  }

  @Test
  void rejectsImageInputWhenServiceDoesNotDeclareMultimodalCapability() {
    RecordingClient client = new RecordingClient(completedStream("data:[DONE]\n\n"));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            new OpenAiCompatibleAdapterConfig(
                service(false),
                4096,
                Duration.ofSeconds(1),
                "gpt-compatible",
                "Authorization",
                "token-1"),
            client);

    OpenAiCompatibleAdapterException error =
        assertThrows(
            OpenAiCompatibleAdapterException.class,
            () -> adapter.execute(multimodalCommand(), context()));

    assertTrue(error.getMessage().contains("does not support multimodal input"));
  }

  @Test
  void mapsReasoningToExtensionWithoutChangingCoreEventEnum() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"id":"chatcmpl-1","choices":[{"index":0,"delta":{"reasoning_content":"think"}}]}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    assertEquals(EventType.EXTENSION, events.get(0).eventType());
    assertEquals("reasoning.delta", events.get(0).payload().get("extensionType"));
    assertEquals("think", events.get(0).payload().get("content"));
  }

  @Test
  void mapsResponsesLifecycleEventsToExecutionEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.queued","event_id":"event_lifecycle_1","response_id":"resp_1","response":{"id":"resp_1","status":"queued"},"sequence_number":1}

                data:{"type":"response.created","event_id":"event_lifecycle_2","response_id":"resp_1","response":{"id":"resp_1","status":"in_progress"},"sequence_number":2}

                data:{"type":"response.in_progress","event_id":"event_lifecycle_3","response_id":"resp_1","response":{"id":"resp_1","status":"in_progress"},"sequence_number":3}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(3, events.size());
    assertEquals(EventType.EXECUTION_ACCEPTED, events.get(0).eventType());
    assertEquals("response.queued", events.get(0).payload().get("downstreamEventType"));
    assertEquals("queued", events.get(0).payload().get("downstreamStatus"));
    assertEquals("resp_1", events.get(0).payload().get("responseId"));
    assertResponseEventMetadata(events.get(0), "response.queued", "event_lifecycle_1", 1L);
    assertEquals("resp_1", events.get(0).sourceEventId());
    assertEquals(EventType.EXECUTION_STARTED, events.get(1).eventType());
    assertEquals("response.created", events.get(1).payload().get("downstreamEventType"));
    assertEquals("in_progress", events.get(1).payload().get("downstreamStatus"));
    assertResponseEventMetadata(events.get(1), "response.created", "event_lifecycle_2", 2L);
    assertEquals(EventType.EXECUTION_RUNNING, events.get(2).eventType());
    assertEquals("response.in_progress", events.get(2).payload().get("downstreamEventType"));
    assertResponseEventMetadata(events.get(2), "response.in_progress", "event_lifecycle_3", 3L);
  }

  @Test
  void mapsResponsesStreamingEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_text.delta","event_id":"event_output_text_1","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hello","logprobs":[{"token":"hello","logprob":-0.1,"bytes":[104,101,108,108,111],"top_logprobs":[{"token":"hello","logprob":-0.1}]}],"sequence_number":1}

                data:{"type":"response.output_text.done","event_id":"event_output_text_2","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"text":"hello world","logprobs":[{"token":"world","logprob":-0.2}],"sequence_number":2}

                data:{"type":"response.reasoning_text.delta","event_id":"event_reasoning_1","response_id":"resp_1","item_id":"rs_1","output_index":0,"content_index":0,"delta":"think","sequence_number":3}

                data:{"type":"response.reasoning_text.done","event_id":"event_reasoning_2","response_id":"resp_1","item_id":"rs_1","output_index":0,"content_index":0,"text":"think done","sequence_number":4}

                data:{"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":1,"delta":"{\\"city\\":","sequence_number":5}

                data:{"type":"response.completed","event_id":"event_lifecycle_4","response_id":"resp_1","response":{"id":"resp_1","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}},"sequence_number":6}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(7, events.size());
    assertEquals(EventType.TEXT_DELTA, events.get(0).eventType());
    assertEquals("hello", events.get(0).payload().get("content"));
    assertEquals("response.output_text", events.get(0).payload().get("contentType"));
    assertEquals("delta", events.get(0).payload().get("phase"));
    assertEquals("msg_1", events.get(0).payload().get("itemId"));
    assertEquals(0, events.get(0).payload().get("outputIndex"));
    assertEquals(0, events.get(0).payload().get("contentIndex"));
    assertEquals("response.output_text.delta", events.get(0).payload().get("downstreamEventType"));
    assertEquals(1L, events.get(0).payload().get("downstreamSequenceNumber"));
    List<?> deltaLogprobs = assertInstanceOf(List.class, events.get(0).payload().get("logprobs"));
    Map<?, ?> deltaLogprob = assertInstanceOf(Map.class, deltaLogprobs.getFirst());
    assertEquals("hello", deltaLogprob.get("token"));
    assertEquals(-0.1, deltaLogprob.get("logprob"));
    List<?> topLogprobs = assertInstanceOf(List.class, deltaLogprob.get("top_logprobs"));
    assertEquals(1, topLogprobs.size());
    assertEquals("msg_1", events.get(0).sourceEventId());
    assertResponseEventMetadata(
        events.get(0), "response.output_text.delta", "event_output_text_1", 1L);
    assertEquals(EventType.TEXT_END, events.get(1).eventType());
    assertEquals("hello world", events.get(1).payload().get("content"));
    assertEquals("done", events.get(1).payload().get("phase"));
    assertEquals("response.output_text.done", events.get(1).payload().get("downstreamEventType"));
    List<?> doneLogprobs = assertInstanceOf(List.class, events.get(1).payload().get("logprobs"));
    Map<?, ?> doneLogprob = assertInstanceOf(Map.class, doneLogprobs.getFirst());
    assertEquals("world", doneLogprob.get("token"));
    assertEquals(-0.2, doneLogprob.get("logprob"));
    assertResponseEventMetadata(
        events.get(1), "response.output_text.done", "event_output_text_2", 2L);
    assertReasoningTextEvent(
        events.get(2),
        "reasoning.delta",
        "think",
        "response.reasoning_text.delta",
        "event_reasoning_1",
        3L);
    assertReasoningTextEvent(
        events.get(3),
        "reasoning.done",
        "think done",
        "response.reasoning_text.done",
        "event_reasoning_2",
        4L);
    assertEquals(EventType.TOOL_ARGUMENTS_DELTA, events.get(4).eventType());
    assertEquals(1, events.get(4).payload().get("index"));
    assertEquals("fc_1", events.get(4).payload().get("toolCallId"));
    assertEquals("{\"city\":", events.get(4).payload().get("arguments"));
    assertEquals(EventType.USAGE_UPDATED, events.get(5).eventType());
    assertEquals(5, events.get(5).payload().get("total_tokens"));
    assertResponseEventMetadata(events.get(5), "response.completed", "event_lifecycle_4", 6L);
    assertEquals(EventType.EXECUTION_COMPLETED, events.get(6).eventType());
    assertResponseEventMetadata(events.get(6), "response.completed", "event_lifecycle_4", 6L);
  }

  @Test
  void mapsResponsesReasoningSummaryEventsToExtensions() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.reasoning_summary_text.delta","event_id":"event_summary_1","response_id":"resp_1","item_id":"rs_1","output_index":0,"summary_index":1,"delta":"summary ","sequence_number":1}

                data:{"type":"response.reasoning_summary_text.done","event_id":"event_summary_2","response_id":"resp_1","item_id":"rs_1","output_index":0,"summary_index":1,"text":"summary done","sequence_number":2}

                data:{"type":"response.reasoning_summary_part.added","event_id":"event_summary_3","response_id":"resp_1","item_id":"rs_1","output_index":0,"summary_index":2,"part":{"type":"summary_text","text":""},"sequence_number":3}

                data:{"type":"response.reasoning_summary_part.done","event_id":"event_summary_4","response_id":"resp_1","item_id":"rs_1","output_index":0,"summary_index":2,"part":{"type":"summary_text","text":"part done"},"sequence_number":4}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(4, events.size());
    assertReasoningSummaryEvent(
        events.get(0),
        "reasoning.summary.delta",
        "summary ",
        1,
        "response.reasoning_summary_text.delta",
        "",
        "event_summary_1",
        1L);
    assertReasoningSummaryEvent(
        events.get(1),
        "reasoning.summary.done",
        "summary done",
        1,
        "response.reasoning_summary_text.done",
        "",
        "event_summary_2",
        2L);
    assertReasoningSummaryEvent(
        events.get(2),
        "reasoning.summary.part.added",
        "",
        2,
        "response.reasoning_summary_part.added",
        "summary_text",
        "event_summary_3",
        3L);
    assertReasoningSummaryEvent(
        events.get(3),
        "reasoning.summary.part.done",
        "part done",
        2,
        "response.reasoning_summary_part.done",
        "summary_text",
        "event_summary_4",
        4L);
  }

  @Test
  void mapsResponsesFunctionCallLifecycleToToolEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","event_id":"event_output_item_1","response_id":"resp_1","output_index":1,"item":{"id":"fc_1","type":"function_call","name":"lookup","call_id":"call_1"},"sequence_number":1}

                data:{"type":"response.function_call_arguments.done","event_id":"event_function_args_1","response_id":"resp_1","item_id":"fc_1","output_index":1,"name":"lookup","arguments":"{\\"city\\":\\"sh\\"}","sequence_number":2}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(2, events.size());
    assertEquals(EventType.TOOL_START, events.get(0).eventType());
    assertEquals(1, events.get(0).payload().get("index"));
    assertEquals("fc_1", events.get(0).payload().get("toolCallId"));
    assertEquals("call_1", events.get(0).payload().get("callId"));
    assertEquals("lookup", events.get(0).payload().get("name"));
    assertResponseEventMetadata(
        events.get(0), "response.output_item.added", "event_output_item_1", 1L);
    assertEquals("fc_1", events.get(0).sourceEventId());
    assertEquals(EventType.TOOL_END, events.get(1).eventType());
    assertEquals(1, events.get(1).payload().get("index"));
    assertEquals("fc_1", events.get(1).payload().get("toolCallId"));
    assertEquals("lookup", events.get(1).payload().get("name"));
    assertEquals("function", events.get(1).payload().get("toolType"));
    assertEquals("{\"city\":\"sh\"}", events.get(1).payload().get("arguments"));
    assertResponseEventMetadata(
        events.get(1), "response.function_call_arguments.done", "event_function_args_1", 2L);
    assertEquals("fc_1", events.get(1).sourceEventId());
  }

  @Test
  void mapsResponsesFunctionCallArgumentsDeltaWithMetadata() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.function_call_arguments.delta","event_id":"event_function_args_2","response_id":"resp_1","item_id":"fc_1","output_index":1,"name":"lookup","delta":"{\\"city","sequence_number":3}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    assertEquals(EventType.TOOL_ARGUMENTS_DELTA, events.getFirst().eventType());
    assertEquals(1, events.getFirst().payload().get("index"));
    assertEquals("fc_1", events.getFirst().payload().get("toolCallId"));
    assertEquals("function", events.getFirst().payload().get("toolType"));
    assertEquals("lookup", events.getFirst().payload().get("name"));
    assertEquals("{\"city", events.getFirst().payload().get("arguments"));
    assertResponseEventMetadata(
        events.getFirst(), "response.function_call_arguments.delta", "event_function_args_2", 3L);
    assertEquals("fc_1", events.getFirst().sourceEventId());
  }

  @Test
  void mapsResponsesOutputItemDoneFunctionCallToToolEnd() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.done","event_id":"event_output_item_2","response_id":"resp_1","output_index":1,"item":{"id":"fc_1","type":"function_call","name":"lookup","arguments":"{\\"city\\":\\"sh\\"}"},"sequence_number":1}

                data:{"type":"response.output_item.added","event_id":"event_output_item_3","response_id":"resp_1","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"in_progress","content":[]},"sequence_number":2}

                data:{"type":"response.output_item.done","event_id":"event_output_item_4","response_id":"resp_1","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello","annotations":[]}]},"sequence_number":3}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(3, events.size());
    assertEquals(EventType.TOOL_END, events.getFirst().eventType());
    assertEquals("fc_1", events.getFirst().payload().get("toolCallId"));
    assertEquals("lookup", events.getFirst().payload().get("name"));
    assertEquals("{\"city\":\"sh\"}", events.getFirst().payload().get("arguments"));
    assertResponseEventMetadata(
        events.getFirst(), "response.output_item.done", "event_output_item_2", 1L);
    assertEquals("fc_1", events.getFirst().sourceEventId());
    assertOutputItemExtension(
        events.get(1), "output_item.added", "msg_1", "message", "in_progress", 0);
    assertResponseEventMetadata(
        events.get(1), "response.output_item.added", "event_output_item_3", 2L);
    assertOutputItemExtension(
        events.get(2), "output_item.done", "msg_1", "message", "completed", 0);
    assertResponseEventMetadata(
        events.get(2), "response.output_item.done", "event_output_item_4", 3L);
  }

  @Test
  void mapsResponsesMcpApprovalItemsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","event_id":"event_output_item_5","response_id":"resp_1","output_index":4,"item":{"id":"appr_1","type":"mcp_approval_request","name":"deploy","server_label":"ops","arguments":"{\\"env\\":\\"prod\\"}"},"sequence_number":1}

                data:{"type":"response.output_item.done","event_id":"event_output_item_6","response_id":"resp_1","output_index":4,"item":{"id":"appr_1","type":"mcp_approval_request","name":"deploy","server_label":"ops","arguments":"{\\"env\\":\\"prod\\"}"},"sequence_number":2}

                data:{"type":"response.output_item.added","event_id":"event_output_item_7","response_id":"resp_1","output_index":5,"item":{"id":"appr_resp_1","type":"mcp_approval_response","approval_request_id":"appr_1","approve":true,"reason":"approved by operator"},"sequence_number":3}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(3, events.size());
    assertEquals(EventType.APPROVAL_REQUIRED, events.get(0).eventType());
    assertEquals("mcp.approval_request.added", events.get(0).payload().get("extensionType"));
    assertEquals("appr_1", events.get(0).payload().get("approvalRequestId"));
    assertEquals("mcp", events.get(0).payload().get("toolType"));
    assertEquals("deploy", events.get(0).payload().get("name"));
    assertEquals("ops", events.get(0).payload().get("serverLabel"));
    assertEquals("{\"env\":\"prod\"}", events.get(0).payload().get("arguments"));
    assertResponseEventMetadata(
        events.get(0), "response.output_item.added", "event_output_item_5", 1L);
    assertEquals("appr_1", events.get(0).sourceEventId());
    assertEquals(EventType.EXTENSION, events.get(1).eventType());
    assertEquals("mcp.approval_request.done", events.get(1).payload().get("extensionType"));
    assertEquals("appr_1", events.get(1).payload().get("approvalRequestId"));
    assertResponseEventMetadata(
        events.get(1), "response.output_item.done", "event_output_item_6", 2L);
    assertEquals(EventType.EXTENSION, events.get(2).eventType());
    assertEquals("mcp.approval_response.added", events.get(2).payload().get("extensionType"));
    assertEquals("appr_resp_1", events.get(2).payload().get("approvalResponseId"));
    assertEquals("appr_1", events.get(2).payload().get("approvalRequestId"));
    assertEquals(true, events.get(2).payload().get("approved"));
    assertEquals("approved by operator", events.get(2).payload().get("reason"));
    assertResponseEventMetadata(
        events.get(2), "response.output_item.added", "event_output_item_7", 3L);
  }

  @Test
  void mapsResponsesCustomToolCallEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","output_index":2,"item":{"id":"ctc_1","type":"custom_tool_call","call_id":"call_custom_1","name":"render_diagram"},"sequence_number":1}

                data:{"type":"response.custom_tool_call_input.delta","event_id":"event_custom_input_1","response_id":"resp_1","output_index":2,"item_id":"ctc_1","delta":"draw ","sequence_number":2}

                data:{"type":"response.custom_tool_call_input.done","event_id":"event_custom_input_2","response_id":"resp_1","output_index":2,"item_id":"ctc_1","input":"draw a graph","sequence_number":3}

                data:{"type":"response.output_item.done","output_index":2,"item":{"id":"ctc_1","type":"custom_tool_call","call_id":"call_custom_1","name":"render_diagram","input":"draw a graph"},"sequence_number":4}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(4, events.size());
    assertEquals(EventType.TOOL_START, events.get(0).eventType());
    assertEquals("ctc_1", events.get(0).payload().get("toolCallId"));
    assertEquals("call_custom_1", events.get(0).payload().get("callId"));
    assertEquals("custom_tool", events.get(0).payload().get("toolType"));
    assertEquals("render_diagram", events.get(0).payload().get("name"));
    assertEquals("ctc_1", events.get(0).sourceEventId());
    assertEquals(EventType.TOOL_ARGUMENTS_DELTA, events.get(1).eventType());
    assertEquals("ctc_1", events.get(1).payload().get("toolCallId"));
    assertEquals("custom_tool", events.get(1).payload().get("toolType"));
    assertEquals("draw ", events.get(1).payload().get("input"));
    assertEquals(
        "response.custom_tool_call_input.delta",
        events.get(1).payload().get("downstreamEventType"));
    assertEquals("resp_1", events.get(1).payload().get("responseId"));
    assertEquals("event_custom_input_1", events.get(1).payload().get("downstreamEventId"));
    assertEquals(2L, events.get(1).payload().get("downstreamSequenceNumber"));
    assertEquals("ctc_1", events.get(1).sourceEventId());
    assertCustomToolInputDoneEvent(events.get(2), "draw a graph", 2, "event_custom_input_2", 3L);
    assertEquals(EventType.TOOL_END, events.get(3).eventType());
    assertEquals("ctc_1", events.get(3).payload().get("toolCallId"));
    assertEquals("call_custom_1", events.get(3).payload().get("callId"));
    assertEquals("custom_tool", events.get(3).payload().get("toolType"));
    assertEquals("render_diagram", events.get(3).payload().get("name"));
    assertEquals("draw a graph", events.get(3).payload().get("input"));
    assertEquals("ctc_1", events.get(3).sourceEventId());
  }

  @Test
  void mapsResponsesComputerCallItemsToToolEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","output_index":6,"item":{"id":"cc_1","type":"computer_call","call_id":"call_computer_1","status":"in_progress","action":{"type":"click","x":10,"y":20},"pending_safety_checks":[{"id":"safe_1","code":"confirm_click","message":"Confirm click"}]},"sequence_number":1}

                data:{"type":"response.output_item.done","output_index":6,"item":{"id":"cc_1","type":"computer_call","call_id":"call_computer_1","status":"completed","actions":[{"type":"click","x":10,"y":20},{"type":"keypress","keys":["ENTER"]}],"pending_safety_checks":[]},"sequence_number":2}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(2, events.size());
    assertEquals(EventType.TOOL_START, events.get(0).eventType());
    assertEquals("cc_1", events.get(0).payload().get("toolCallId"));
    assertEquals("call_computer_1", events.get(0).payload().get("callId"));
    assertEquals("computer", events.get(0).payload().get("toolType"));
    assertEquals("in_progress", events.get(0).payload().get("status"));
    assertEquals(6, events.get(0).payload().get("index"));
    Map<?, ?> action = assertInstanceOf(Map.class, events.get(0).payload().get("action"));
    assertEquals("click", action.get("type"));
    assertEquals(10, action.get("x"));
    List<?> pendingSafetyChecks =
        assertInstanceOf(List.class, events.get(0).payload().get("pendingSafetyChecks"));
    assertEquals(1, pendingSafetyChecks.size());
    assertEquals("cc_1", events.get(0).sourceEventId());

    assertEquals(EventType.TOOL_END, events.get(1).eventType());
    assertEquals("cc_1", events.get(1).payload().get("toolCallId"));
    assertEquals("call_computer_1", events.get(1).payload().get("callId"));
    assertEquals("computer", events.get(1).payload().get("toolType"));
    assertEquals("completed", events.get(1).payload().get("status"));
    List<?> actions = assertInstanceOf(List.class, events.get(1).payload().get("actions"));
    assertEquals(2, actions.size());
    Map<?, ?> keypress = assertInstanceOf(Map.class, actions.get(1));
    assertEquals("keypress", keypress.get("type"));
    assertEquals("cc_1", events.get(1).sourceEventId());
  }

  @Test
  void mapsResponsesComputerCallOutputItemsToArtifactEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","event_id":"event_output_item_8","response_id":"resp_1","output_index":7,"item":{"id":"cco_1","type":"computer_call_output","call_id":"call_computer_1","status":"in_progress","output":{"type":"computer_screenshot","image_url":"https://example.com/screen.png"},"acknowledged_safety_checks":[{"id":"safe_1","code":"confirm_click","message":"Confirmed"}]},"sequence_number":1}

                data:{"type":"response.output_item.done","event_id":"event_output_item_9","response_id":"resp_1","output_index":7,"item":{"id":"cco_1","type":"computer_call_output","call_id":"call_computer_1","status":"completed","output":{"type":"computer_screenshot","file_id":"file_screen_1"},"acknowledged_safety_checks":[]},"sequence_number":2}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(2, events.size());
    assertEquals(EventType.ARTIFACT_CREATED, events.get(0).eventType());
    assertEquals("computer_call_output.added", events.get(0).payload().get("artifactType"));
    assertEquals("cco_1", events.get(0).payload().get("itemId"));
    assertEquals("computer_call_output", events.get(0).payload().get("itemType"));
    assertEquals("computer", events.get(0).payload().get("toolType"));
    assertEquals("call_computer_1", events.get(0).payload().get("callId"));
    assertEquals("in_progress", events.get(0).payload().get("status"));
    assertEquals(7, events.get(0).payload().get("index"));
    Map<?, ?> createdOutput = assertInstanceOf(Map.class, events.get(0).payload().get("output"));
    assertEquals("computer_screenshot", createdOutput.get("type"));
    assertEquals("https://example.com/screen.png", createdOutput.get("image_url"));
    List<?> acknowledgedSafetyChecks =
        assertInstanceOf(List.class, events.get(0).payload().get("acknowledgedSafetyChecks"));
    assertEquals(1, acknowledgedSafetyChecks.size());
    assertResponseEventMetadata(
        events.get(0), "response.output_item.added", "event_output_item_8", 1L);
    assertEquals("cco_1", events.get(0).sourceEventId());

    assertEquals(EventType.ARTIFACT_COMPLETED, events.get(1).eventType());
    assertEquals("computer_call_output.done", events.get(1).payload().get("artifactType"));
    assertEquals("cco_1", events.get(1).payload().get("itemId"));
    assertEquals("computer_call_output", events.get(1).payload().get("itemType"));
    assertEquals("computer", events.get(1).payload().get("toolType"));
    assertEquals("call_computer_1", events.get(1).payload().get("callId"));
    assertEquals("completed", events.get(1).payload().get("status"));
    Map<?, ?> completedOutput = assertInstanceOf(Map.class, events.get(1).payload().get("output"));
    assertEquals("computer_screenshot", completedOutput.get("type"));
    assertEquals("file_screen_1", completedOutput.get("file_id"));
    assertResponseEventMetadata(
        events.get(1), "response.output_item.done", "event_output_item_9", 2L);
    assertEquals("cco_1", events.get(1).sourceEventId());
  }

  @Test
  void mapsResponsesShellCallItemsToToolEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","output_index":8,"item":{"id":"lsh_1","type":"local_shell_call","call_id":"call_shell_1","status":"in_progress","action":{"type":"exec","command":["npm","test"],"working_directory":"/workspace","timeout_ms":1000}},"sequence_number":1}

                data:{"type":"response.output_item.done","output_index":8,"item":{"id":"lsh_1","type":"local_shell_call","call_id":"call_shell_1","status":"completed","action":{"type":"exec","command":["npm","test"],"env":{"CI":"true"}}},"sequence_number":2}

                data:{"type":"response.output_item.added","output_index":9,"item":{"id":"sh_1","type":"shell_call","call_id":"call_shell_2","created_by":"assistant","status":"in_progress","action":{"commands":["echo hi"],"timeout_ms":500}},"sequence_number":3}

                data:{"type":"response.output_item.done","output_index":9,"item":{"id":"sh_1","type":"shell_call","call_id":"call_shell_2","created_by":"assistant","status":"completed","action":{"commands":["echo hi"],"max_output_length":2000}},"sequence_number":4}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(4, events.size());
    assertEquals(EventType.TOOL_START, events.get(0).eventType());
    assertEquals("lsh_1", events.get(0).payload().get("toolCallId"));
    assertEquals("local_shell", events.get(0).payload().get("toolType"));
    assertEquals("call_shell_1", events.get(0).payload().get("callId"));
    assertEquals("in_progress", events.get(0).payload().get("status"));
    Map<?, ?> localAction = assertInstanceOf(Map.class, events.get(0).payload().get("action"));
    assertEquals("exec", localAction.get("type"));
    assertEquals(8, events.get(0).payload().get("index"));

    assertEquals(EventType.TOOL_END, events.get(1).eventType());
    assertEquals("lsh_1", events.get(1).payload().get("toolCallId"));
    assertEquals("local_shell", events.get(1).payload().get("toolType"));
    assertEquals("completed", events.get(1).payload().get("status"));
    assertEquals("lsh_1", events.get(1).sourceEventId());

    assertEquals(EventType.TOOL_START, events.get(2).eventType());
    assertEquals("sh_1", events.get(2).payload().get("toolCallId"));
    assertEquals("shell", events.get(2).payload().get("toolType"));
    assertEquals("assistant", events.get(2).payload().get("createdBy"));
    Map<?, ?> shellAction = assertInstanceOf(Map.class, events.get(2).payload().get("action"));
    assertEquals(500, shellAction.get("timeout_ms"));

    assertEquals(EventType.TOOL_END, events.get(3).eventType());
    assertEquals("sh_1", events.get(3).payload().get("toolCallId"));
    assertEquals("shell", events.get(3).payload().get("toolType"));
    assertEquals("completed", events.get(3).payload().get("status"));
    assertEquals("sh_1", events.get(3).sourceEventId());
  }

  @Test
  void mapsResponsesShellCallOutputItemsToToolResultEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_item.added","event_id":"event_output_item_10","response_id":"resp_1","output_index":10,"item":{"id":"lsho_1","type":"local_shell_call_output","status":"completed","output":"{\\"stdout\\":\\"ok\\",\\"stderr\\":\\"\\"}"},"sequence_number":1}

                data:{"type":"response.output_item.done","event_id":"event_output_item_11","response_id":"resp_1","output_index":11,"item":{"id":"sho_1","type":"shell_call_output","call_id":"call_shell_2","created_by":"assistant","status":"completed","max_output_length":2000,"output":[{"stdout":"ok","stderr":"","outcome":{"type":"exit","exit_code":0}}]},"sequence_number":2}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(2, events.size());
    assertEquals(EventType.TOOL_RESULT, events.get(0).eventType());
    assertEquals("shell_call_output.added", events.get(0).payload().get("phase"));
    assertEquals("lsho_1", events.get(0).payload().get("toolCallId"));
    assertEquals("local_shell", events.get(0).payload().get("toolType"));
    assertEquals("completed", events.get(0).payload().get("status"));
    assertEquals("{\"stdout\":\"ok\",\"stderr\":\"\"}", events.get(0).payload().get("output"));
    assertEquals(10, events.get(0).payload().get("index"));
    assertResponseEventMetadata(
        events.get(0), "response.output_item.added", "event_output_item_10", 1L);

    assertEquals(EventType.TOOL_RESULT, events.get(1).eventType());
    assertEquals("shell_call_output.done", events.get(1).payload().get("phase"));
    assertEquals("sho_1", events.get(1).payload().get("toolCallId"));
    assertEquals("shell", events.get(1).payload().get("toolType"));
    assertEquals("call_shell_2", events.get(1).payload().get("callId"));
    assertEquals("assistant", events.get(1).payload().get("createdBy"));
    assertEquals(2000, events.get(1).payload().get("maxOutputLength"));
    List<?> output = assertInstanceOf(List.class, events.get(1).payload().get("output"));
    Map<?, ?> firstOutput = assertInstanceOf(Map.class, output.getFirst());
    Map<?, ?> outcome = assertInstanceOf(Map.class, firstOutput.get("outcome"));
    assertEquals("exit", outcome.get("type"));
    assertEquals(0, outcome.get("exit_code"));
    assertResponseEventMetadata(
        events.get(1), "response.output_item.done", "event_output_item_11", 2L);
    assertEquals("sho_1", events.get(1).sourceEventId());
  }

  @Test
  void mapsResponsesBuiltinToolStatusEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.file_search_call.in_progress","event_id":"event_search_1","response_id":"resp_1","output_index":0,"item_id":"fs_1","sequence_number":1}

                data:{"type":"response.file_search_call.searching","event_id":"event_search_2","response_id":"resp_1","output_index":0,"item_id":"fs_1","sequence_number":2}

                data:{"type":"response.file_search_call.completed","event_id":"event_search_3","response_id":"resp_1","output_index":0,"item_id":"fs_1","sequence_number":3}

                data:{"type":"response.web_search_call.in_progress","event_id":"event_search_4","response_id":"resp_1","output_index":1,"item_id":"ws_1","sequence_number":4}

                data:{"type":"response.web_search_call.searching","event_id":"event_search_5","response_id":"resp_1","output_index":1,"item_id":"ws_1","sequence_number":5}

                data:{"type":"response.web_search_call.completed","event_id":"event_search_6","response_id":"resp_1","output_index":1,"item_id":"ws_1","sequence_number":6}

                data:{"type":"response.file_search_call.failed","event_id":"event_search_7","response_id":"resp_1","output_index":2,"item_id":"fs_2","sequence_number":7}

                data:{"type":"response.web_search_call.incomplete","event_id":"event_search_8","response_id":"resp_1","output_index":3,"item_id":"ws_2","sequence_number":8}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(8, events.size());
    assertBuiltinToolEvent(
        events.get(0), EventType.TOOL_START, "fs_1", "file_search", "in_progress", 0);
    assertResponseEventMetadata(
        events.get(0), "response.file_search_call.in_progress", "event_search_1", 1L);
    assertBuiltinToolEvent(
        events.get(1), EventType.PROGRESS_UPDATED, "fs_1", "file_search", "searching", 0);
    assertResponseEventMetadata(
        events.get(1), "response.file_search_call.searching", "event_search_2", 2L);
    assertBuiltinToolEvent(
        events.get(2), EventType.TOOL_END, "fs_1", "file_search", "completed", 0);
    assertResponseEventMetadata(
        events.get(2), "response.file_search_call.completed", "event_search_3", 3L);
    assertBuiltinToolEvent(
        events.get(3), EventType.TOOL_START, "ws_1", "web_search", "in_progress", 1);
    assertResponseEventMetadata(
        events.get(3), "response.web_search_call.in_progress", "event_search_4", 4L);
    assertBuiltinToolEvent(
        events.get(4), EventType.PROGRESS_UPDATED, "ws_1", "web_search", "searching", 1);
    assertResponseEventMetadata(
        events.get(4), "response.web_search_call.searching", "event_search_5", 5L);
    assertBuiltinToolEvent(events.get(5), EventType.TOOL_END, "ws_1", "web_search", "completed", 1);
    assertResponseEventMetadata(
        events.get(5), "response.web_search_call.completed", "event_search_6", 6L);
    assertBuiltinToolEvent(events.get(6), EventType.TOOL_ERROR, "fs_2", "file_search", "failed", 2);
    assertResponseEventMetadata(
        events.get(6), "response.file_search_call.failed", "event_search_7", 7L);
    assertBuiltinToolEvent(
        events.get(7), EventType.TOOL_ERROR, "ws_2", "web_search", "incomplete", 3);
    assertResponseEventMetadata(
        events.get(7), "response.web_search_call.incomplete", "event_search_8", 8L);
  }

  @Test
  void mapsResponsesCodeInterpreterEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.code_interpreter_call.in_progress","event_id":"event_ci_1","response_id":"resp_1","output_index":2,"item_id":"ci_1","sequence_number":1}

                data:{"type":"response.code_interpreter_call.interpreting","event_id":"event_ci_2","response_id":"resp_1","output_index":2,"item_id":"ci_1","sequence_number":2}

                data:{"type":"response.code_interpreter_call_code.delta","event_id":"event_ci_3","response_id":"resp_1","output_index":2,"item_id":"ci_1","delta":"print(","sequence_number":3}

                data:{"type":"response.code_interpreter_call_code.done","event_id":"event_ci_4","response_id":"resp_1","output_index":2,"item_id":"ci_1","code":"print('done')","sequence_number":4}

                data:{"type":"response.code_interpreter_call.completed","event_id":"event_ci_5","response_id":"resp_1","output_index":2,"item_id":"ci_1","sequence_number":5}

                data:{"type":"response.code_interpreter_call.failed","event_id":"event_ci_6","response_id":"resp_1","output_index":3,"item_id":"ci_2","sequence_number":6}

                data:{"type":"response.code_interpreter_call.incomplete","event_id":"event_ci_7","response_id":"resp_1","output_index":4,"item_id":"ci_3","sequence_number":7}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(7, events.size());
    assertBuiltinToolEvent(
        events.get(0), EventType.TOOL_START, "ci_1", "code_interpreter", "in_progress", 2);
    assertResponseEventMetadata(
        events.get(0), "response.code_interpreter_call.in_progress", "event_ci_1", 1L);
    assertBuiltinToolEvent(
        events.get(1), EventType.PROGRESS_UPDATED, "ci_1", "code_interpreter", "interpreting", 2);
    assertResponseEventMetadata(
        events.get(1), "response.code_interpreter_call.interpreting", "event_ci_2", 2L);
    assertCodeInterpreterCodeEvent(events.get(2), "code_interpreter.code.delta", "print(", 2);
    assertResponseEventMetadata(
        events.get(2), "response.code_interpreter_call_code.delta", "event_ci_3", 3L);
    assertCodeInterpreterCodeEvent(events.get(3), "code_interpreter.code.done", "print('done')", 2);
    assertResponseEventMetadata(
        events.get(3), "response.code_interpreter_call_code.done", "event_ci_4", 4L);
    assertBuiltinToolEvent(
        events.get(4), EventType.TOOL_END, "ci_1", "code_interpreter", "completed", 2);
    assertResponseEventMetadata(
        events.get(4), "response.code_interpreter_call.completed", "event_ci_5", 5L);
    assertBuiltinToolEvent(
        events.get(5), EventType.TOOL_ERROR, "ci_2", "code_interpreter", "failed", 3);
    assertResponseEventMetadata(
        events.get(5), "response.code_interpreter_call.failed", "event_ci_6", 6L);
    assertBuiltinToolEvent(
        events.get(6), EventType.TOOL_ERROR, "ci_3", "code_interpreter", "incomplete", 4);
    assertResponseEventMetadata(
        events.get(6), "response.code_interpreter_call.incomplete", "event_ci_7", 7L);
  }

  @Test
  void mapsResponsesMcpEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.mcp_call_arguments.delta","event_id":"event_mcp_args_1","response_id":"resp_1","output_index":3,"item_id":"mcp_1","delta":"{\\"query\\":","sequence_number":1}

                data:{"type":"response.mcp_call_arguments.done","event_id":"event_mcp_args_2","response_id":"resp_1","output_index":3,"item_id":"mcp_1","arguments":"{\\"query\\":\\"shanghai\\"}","sequence_number":2}

                data:{"type":"response.mcp_call.in_progress","event_id":"event_mcp_call_3","response_id":"resp_1","output_index":3,"item_id":"mcp_1","sequence_number":3}

                data:{"type":"response.mcp_call.calling","event_id":"event_mcp_call_4","response_id":"resp_1","output_index":3,"item_id":"mcp_1","sequence_number":4}

                data:{"type":"response.mcp_call.completed","event_id":"event_mcp_call_5","response_id":"resp_1","output_index":3,"item_id":"mcp_1","sequence_number":5}

                data:{"type":"response.mcp_call.failed","event_id":"event_mcp_call_6","response_id":"resp_1","output_index":4,"item_id":"mcp_2","sequence_number":6}

                data:{"type":"response.mcp_call.incomplete","event_id":"event_mcp_call_7","response_id":"resp_1","output_index":4,"item_id":"mcp_3","sequence_number":7}

                data:{"type":"response.mcp_list_tools.in_progress","event_id":"event_mcp_list_8","response_id":"resp_1","output_index":5,"item_id":"mcpl_1","sequence_number":8}

                data:{"type":"response.mcp_list_tools.completed","event_id":"event_mcp_list_9","response_id":"resp_1","output_index":5,"item_id":"mcpl_1","sequence_number":9}

                data:{"type":"response.mcp_list_tools.failed","event_id":"event_mcp_list_10","response_id":"resp_1","output_index":6,"item_id":"mcpl_2","sequence_number":10}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(10, events.size());
    assertEquals(EventType.TOOL_ARGUMENTS_DELTA, events.get(0).eventType());
    assertEquals("mcp_1", events.get(0).payload().get("toolCallId"));
    assertEquals("mcp", events.get(0).payload().get("toolType"));
    assertEquals("{\"query\":", events.get(0).payload().get("arguments"));
    assertEquals(3, events.get(0).payload().get("index"));
    assertEquals(
        "response.mcp_call_arguments.delta", events.get(0).payload().get("downstreamEventType"));
    assertEquals("resp_1", events.get(0).payload().get("responseId"));
    assertEquals("event_mcp_args_1", events.get(0).payload().get("downstreamEventId"));
    assertEquals(1L, events.get(0).payload().get("downstreamSequenceNumber"));
    assertMcpArgumentsDoneEvent(
        events.get(1), "{\"query\":\"shanghai\"}", 3, "event_mcp_args_2", 2L);
    assertBuiltinToolEvent(events.get(2), EventType.TOOL_START, "mcp_1", "mcp", "in_progress", 3);
    assertResponseEventMetadata(
        events.get(2), "response.mcp_call.in_progress", "event_mcp_call_3", 3L);
    assertBuiltinToolEvent(events.get(3), EventType.PROGRESS_UPDATED, "mcp_1", "mcp", "calling", 3);
    assertResponseEventMetadata(events.get(3), "response.mcp_call.calling", "event_mcp_call_4", 4L);
    assertBuiltinToolEvent(events.get(4), EventType.TOOL_END, "mcp_1", "mcp", "completed", 3);
    assertResponseEventMetadata(
        events.get(4), "response.mcp_call.completed", "event_mcp_call_5", 5L);
    assertBuiltinToolEvent(events.get(5), EventType.TOOL_ERROR, "mcp_2", "mcp", "failed", 4);
    assertResponseEventMetadata(events.get(5), "response.mcp_call.failed", "event_mcp_call_6", 6L);
    assertBuiltinToolEvent(events.get(6), EventType.TOOL_ERROR, "mcp_3", "mcp", "incomplete", 4);
    assertResponseEventMetadata(
        events.get(6), "response.mcp_call.incomplete", "event_mcp_call_7", 7L);
    assertBuiltinToolEvent(
        events.get(7), EventType.TOOL_START, "mcpl_1", "mcp_list_tools", "in_progress", 5);
    assertResponseEventMetadata(
        events.get(7), "response.mcp_list_tools.in_progress", "event_mcp_list_8", 8L);
    assertBuiltinToolEvent(
        events.get(8), EventType.TOOL_END, "mcpl_1", "mcp_list_tools", "completed", 5);
    assertResponseEventMetadata(
        events.get(8), "response.mcp_list_tools.completed", "event_mcp_list_9", 9L);
    assertBuiltinToolEvent(
        events.get(9), EventType.TOOL_ERROR, "mcpl_2", "mcp_list_tools", "failed", 6);
    assertResponseEventMetadata(
        events.get(9), "response.mcp_list_tools.failed", "event_mcp_list_10", 10L);
  }

  @Test
  void mapsResponsesImageGenerationEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.image_generation_call.in_progress","event_id":"event_img_1","response_id":"resp_1","output_index":4,"item_id":"img_1","sequence_number":1}

                data:{"type":"response.image_generation_call.generating","event_id":"event_img_2","response_id":"resp_1","output_index":4,"item_id":"img_1","sequence_number":2}

                data:{"type":"response.image_generation_call.partial_image","event_id":"event_img_3","response_id":"resp_1","output_index":4,"item_id":"img_1","partial_image_index":0,"partial_image_b64":"abc123","sequence_number":3}

                data:{"type":"response.image_generation_call.completed","event_id":"event_img_4","response_id":"resp_1","output_index":4,"item_id":"img_1","sequence_number":4}

                data:{"type":"response.image_generation_call.failed","event_id":"event_img_5","response_id":"resp_1","output_index":5,"item_id":"img_2","sequence_number":5}

                data:{"type":"response.image_generation_call.incomplete","event_id":"event_img_6","response_id":"resp_1","output_index":6,"item_id":"img_3","sequence_number":6}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(6, events.size());
    assertBuiltinToolEvent(
        events.get(0), EventType.TOOL_START, "img_1", "image_generation", "in_progress", 4);
    assertResponseEventMetadata(
        events.get(0), "response.image_generation_call.in_progress", "event_img_1", 1L);
    assertBuiltinToolEvent(
        events.get(1), EventType.PROGRESS_UPDATED, "img_1", "image_generation", "generating", 4);
    assertResponseEventMetadata(
        events.get(1), "response.image_generation_call.generating", "event_img_2", 2L);
    assertEquals(EventType.ARTIFACT_UPDATED, events.get(2).eventType());
    assertEquals(
        "response.image_generation.partial_image", events.get(2).payload().get("artifactType"));
    assertEquals("img_1", events.get(2).payload().get("itemId"));
    assertEquals(4, events.get(2).payload().get("outputIndex"));
    assertEquals(0, events.get(2).payload().get("partialImageIndex"));
    assertEquals("abc123", events.get(2).payload().get("imageB64"));
    assertResponseEventMetadata(
        events.get(2), "response.image_generation_call.partial_image", "event_img_3", 3L);
    assertEquals("img_1", events.get(2).sourceEventId());
    assertBuiltinToolEvent(
        events.get(3), EventType.TOOL_END, "img_1", "image_generation", "completed", 4);
    assertResponseEventMetadata(
        events.get(3), "response.image_generation_call.completed", "event_img_4", 4L);
    assertBuiltinToolEvent(
        events.get(4), EventType.TOOL_ERROR, "img_2", "image_generation", "failed", 5);
    assertResponseEventMetadata(
        events.get(4), "response.image_generation_call.failed", "event_img_5", 5L);
    assertBuiltinToolEvent(
        events.get(5), EventType.TOOL_ERROR, "img_3", "image_generation", "incomplete", 6);
    assertResponseEventMetadata(
        events.get(5), "response.image_generation_call.incomplete", "event_img_6", 6L);
  }

  @Test
  void mapsImagesStreamingPartialImageEventToArtifactUpdate() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"image_generation.partial_image","b64_json":"img-part-1","created_at":1620000000,"background":"transparent","output_format":"png","partial_image_index":2,"quality":"high","size":"1024x1024"}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    GatewayEvent event = events.getFirst();
    assertEquals(EventType.ARTIFACT_UPDATED, event.eventType());
    assertEquals("image_generation.partial_image", event.payload().get("artifactType"));
    assertEquals("image_generation.partial_image", event.payload().get("downstreamEventType"));
    assertEquals("img-part-1", event.payload().get("imageB64"));
    assertEquals(2, event.payload().get("partialImageIndex"));
    assertEquals(1620000000L, event.payload().get("createdAt"));
    assertEquals("transparent", event.payload().get("background"));
    assertEquals("png", event.payload().get("outputFormat"));
    assertEquals("high", event.payload().get("quality"));
    assertEquals("1024x1024", event.payload().get("size"));
    assertFalse(event.payload().containsKey("responseId"));
    assertFalse(event.payload().containsKey("downstreamSequenceNumber"));
    assertEquals("image_generation.partial_image:1620000000:2", event.sourceEventId());
  }

  @Test
  void mapsResponsesOutputAudioEventsToArtifactAndTranscriptExtensions() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_audio.delta","event_id":"event_audio_1","response_id":"resp_1","item_id":"aud_1","output_index":7,"content_index":2,"delta":"audio-chunk","sequence_number":1}

                data:{"type":"response.output_audio.done","event_id":"event_audio_2","response_id":"resp_1","item_id":"aud_1","output_index":7,"content_index":2,"sequence_number":2}

                data:{"type":"response.output_audio_transcript.delta","event_id":"event_transcript_1","response_id":"resp_1","item_id":"aud_1","output_index":7,"content_index":2,"delta":"hello ","sequence_number":3}

                data:{"type":"response.output_audio_transcript.done","event_id":"event_transcript_2","response_id":"resp_1","item_id":"aud_1","output_index":7,"content_index":2,"transcript":"hello world","sequence_number":4}

                data:{"type":"response.content_part.done","event_id":"event_part_1","response_id":"resp_1","item_id":"aud_1","output_index":7,"content_index":2,"part":{"type":"audio","audio":"final-audio","transcript":"hello world"},"sequence_number":5}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(5, events.size());
    assertEquals(EventType.ARTIFACT_UPDATED, events.get(0).eventType());
    assertEquals("response.output_audio", events.get(0).payload().get("artifactType"));
    assertEquals("delta", events.get(0).payload().get("phase"));
    assertEquals("audio-chunk", events.get(0).payload().get("audioB64"));
    assertEquals("resp_1", events.get(0).payload().get("responseId"));
    assertEquals("event_audio_1", events.get(0).payload().get("downstreamEventId"));
    assertEquals(1L, events.get(0).payload().get("downstreamSequenceNumber"));
    assertEquals("aud_1", events.get(0).sourceEventId());
    assertEquals(EventType.ARTIFACT_COMPLETED, events.get(1).eventType());
    assertEquals("done", events.get(1).payload().get("phase"));
    assertFalse(events.get(1).payload().containsKey("audioB64"));
    assertEquals("event_audio_2", events.get(1).payload().get("downstreamEventId"));
    assertOutputAudioTranscriptEvent(
        events.get(2), "audio.transcript.delta", "hello ", "event_transcript_1", 3L);
    assertOutputAudioTranscriptEvent(
        events.get(3), "audio.transcript.done", "hello world", "event_transcript_2", 4L);
    assertEquals(EventType.ARTIFACT_COMPLETED, events.get(4).eventType());
    assertEquals("response.output_audio", events.get(4).payload().get("artifactType"));
    assertEquals("done", events.get(4).payload().get("phase"));
    assertEquals("final-audio", events.get(4).payload().get("audioB64"));
    assertEquals("hello world", events.get(4).payload().get("transcript"));
    assertEquals("response.content_part.done", events.get(4).payload().get("downstreamEventType"));
    assertEquals("event_part_1", events.get(4).payload().get("downstreamEventId"));
  }

  @Test
  void preservesUnknownResponsesEventsAsRawExtensions() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.future_audio.delta","event_id":"event_future_audio_1","response_id":"resp_1","item_id":"aud_1","output_index":7,"content_index":2,"delta":"abc123","audio":{"format":"mp3"},"sequence_number":42}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    GatewayEvent event = events.getFirst();
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals("response.raw", event.payload().get("extensionType"));
    assertEquals("response.future_audio.delta", event.payload().get("downstreamEventType"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals("event_future_audio_1", event.payload().get("downstreamEventId"));
    assertEquals("aud_1", event.payload().get("itemId"));
    assertEquals(7, event.payload().get("outputIndex"));
    assertEquals(2, event.payload().get("contentIndex"));
    assertEquals(42L, event.payload().get("downstreamSequenceNumber"));
    assertEquals("aud_1", event.sourceEventId());
    Map<?, ?> rawEvent = (Map<?, ?>) event.payload().get("rawEvent");
    assertEquals("response.future_audio.delta", rawEvent.get("type"));
    assertEquals("abc123", rawEvent.get("delta"));
    assertEquals(Map.of("format", "mp3"), rawEvent.get("audio"));
  }

  @Test
  void mapsResponsesOutputTextAnnotationAddedToArtifactCompleted() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.output_text.annotation.added","event_id":"event_annotation_1","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"annotation_index":1,"annotation":{"type":"url_citation","url":"https://example.com","title":"Example","start_index":6,"end_index":13},"sequence_number":1}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    assertEquals(EventType.ARTIFACT_COMPLETED, events.getFirst().eventType());
    assertEquals("msg_1", events.getFirst().sourceEventId());
    assertEquals(
        "response.output_text.annotation", events.getFirst().payload().get("artifactType"));
    assertEquals(
        "response.output_text.annotation.added",
        events.getFirst().payload().get("downstreamEventType"));
    assertEquals("resp_1", events.getFirst().payload().get("responseId"));
    assertEquals("event_annotation_1", events.getFirst().payload().get("downstreamEventId"));
    assertEquals(1L, events.getFirst().payload().get("downstreamSequenceNumber"));
    assertEquals("msg_1", events.getFirst().payload().get("itemId"));
    assertEquals(0, events.getFirst().payload().get("outputIndex"));
    assertEquals(0, events.getFirst().payload().get("contentIndex"));
    assertEquals(1, events.getFirst().payload().get("annotationIndex"));
    Map<?, ?> annotation = (Map<?, ?>) events.getFirst().payload().get("annotation");
    assertEquals("url_citation", annotation.get("type"));
    assertEquals("https://example.com", annotation.get("url"));
    assertEquals("Example", annotation.get("title"));
  }

  @Test
  void mapsResponsesRefusalEventsToExtensions() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.refusal.delta","event_id":"event_refusal_1","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"delta":"I cannot","sequence_number":1}

                data:{"type":"response.refusal.done","event_id":"event_refusal_2","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"refusal":"I cannot help with that.","sequence_number":2}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(2, events.size());
    assertEquals(EventType.EXTENSION, events.get(0).eventType());
    assertEquals("refusal.delta", events.get(0).payload().get("extensionType"));
    assertEquals("I cannot", events.get(0).payload().get("content"));
    assertEquals("msg_1", events.get(0).payload().get("itemId"));
    assertEquals(0, events.get(0).payload().get("outputIndex"));
    assertEquals(0, events.get(0).payload().get("contentIndex"));
    assertEquals("response.refusal.delta", events.get(0).payload().get("downstreamEventType"));
    assertEquals("resp_1", events.get(0).payload().get("responseId"));
    assertEquals("event_refusal_1", events.get(0).payload().get("downstreamEventId"));
    assertEquals(1L, events.get(0).payload().get("downstreamSequenceNumber"));
    assertEquals("msg_1", events.get(0).sourceEventId());
    assertEquals(EventType.EXTENSION, events.get(1).eventType());
    assertEquals("refusal.done", events.get(1).payload().get("extensionType"));
    assertEquals("I cannot help with that.", events.get(1).payload().get("content"));
    assertEquals("msg_1", events.get(1).payload().get("itemId"));
    assertEquals(0, events.get(1).payload().get("outputIndex"));
    assertEquals(0, events.get(1).payload().get("contentIndex"));
    assertEquals("response.refusal.done", events.get(1).payload().get("downstreamEventType"));
    assertEquals("resp_1", events.get(1).payload().get("responseId"));
    assertEquals("event_refusal_2", events.get(1).payload().get("downstreamEventId"));
    assertEquals(2L, events.get(1).payload().get("downstreamSequenceNumber"));
    assertEquals("msg_1", events.get(1).sourceEventId());
  }

  @Test
  void mapsResponsesContentPartLifecycleToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"response.content_part.added","event_id":"event_part_standard_1","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":""},"sequence_number":1}

                data:{"type":"response.content_part.done","event_id":"event_part_standard_2","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"hello"},"sequence_number":2}

                data:{"type":"response.content_part.added","event_id":"event_part_standard_3","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":1,"part":{"type":"refusal","refusal":""},"sequence_number":3}

                data:{"type":"response.content_part.done","event_id":"event_part_standard_4","response_id":"resp_1","item_id":"msg_1","output_index":0,"content_index":1,"part":{"type":"refusal","refusal":"I cannot help with that."},"sequence_number":4}

                data:{"type":"response.content_part.done","event_id":"event_part_standard_5","response_id":"resp_1","item_id":"rs_1","output_index":1,"content_index":0,"part":{"type":"reasoning_text","text":"checked policy"},"sequence_number":5}

                data:{"type":"response.content_part.added","event_id":"event_unknown_part_1","response_id":"resp_1","item_id":"msg_2","output_index":2,"content_index":0,"part":{"type":"future_part","value":"opaque"},"sequence_number":6}

                data:{"type":"response.content_part.done","event_id":"event_unknown_part_2","response_id":"resp_1","item_id":"msg_2","output_index":2,"content_index":0,"part":{"type":"future_part","value":"opaque done","extra":{"nested":true}},"sequence_number":7}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(7, events.size());
    assertEquals(EventType.TEXT_START, events.get(0).eventType());
    assertEquals("response.output_text", events.get(0).payload().get("contentType"));
    assertEquals("msg_1", events.get(0).payload().get("itemId"));
    assertEquals(0, events.get(0).payload().get("contentIndex"));
    assertResponseEventMetadata(
        events.get(0), "response.content_part.added", "event_part_standard_1", 1L);
    assertEquals(EventType.TEXT_END, events.get(1).eventType());
    assertEquals("hello", events.get(1).payload().get("content"));
    assertResponseEventMetadata(
        events.get(1), "response.content_part.done", "event_part_standard_2", 2L);
    assertEquals(EventType.EXTENSION, events.get(2).eventType());
    assertEquals("refusal.part.added", events.get(2).payload().get("contentType"));
    assertResponseEventMetadata(
        events.get(2), "response.content_part.added", "event_part_standard_3", 3L);
    assertEquals(EventType.EXTENSION, events.get(3).eventType());
    assertEquals("refusal.part.done", events.get(3).payload().get("contentType"));
    assertEquals("I cannot help with that.", events.get(3).payload().get("content"));
    assertResponseEventMetadata(
        events.get(3), "response.content_part.done", "event_part_standard_4", 4L);
    assertEquals(EventType.EXTENSION, events.get(4).eventType());
    assertEquals("reasoning.part.done", events.get(4).payload().get("contentType"));
    assertEquals("checked policy", events.get(4).payload().get("content"));
    assertResponseEventMetadata(
        events.get(4), "response.content_part.done", "event_part_standard_5", 5L);
    assertEquals(EventType.EXTENSION, events.get(5).eventType());
    assertEquals("content_part.added", events.get(5).payload().get("extensionType"));
    assertEquals("future_part", events.get(5).payload().get("partType"));
    assertEquals("response.content_part.added", events.get(5).payload().get("downstreamEventType"));
    assertEquals("resp_1", events.get(5).payload().get("responseId"));
    assertEquals("event_unknown_part_1", events.get(5).payload().get("downstreamEventId"));
    assertEquals(6L, events.get(5).payload().get("downstreamSequenceNumber"));
    Map<?, ?> addedPart = assertInstanceOf(Map.class, events.get(5).payload().get("part"));
    assertEquals("opaque", addedPart.get("value"));
    assertEquals("msg_2", events.get(5).sourceEventId());

    assertEquals(EventType.EXTENSION, events.get(6).eventType());
    assertEquals("content_part.done", events.get(6).payload().get("extensionType"));
    assertEquals("future_part", events.get(6).payload().get("partType"));
    Map<?, ?> donePart = assertInstanceOf(Map.class, events.get(6).payload().get("part"));
    Map<?, ?> extra = assertInstanceOf(Map.class, donePart.get("extra"));
    assertEquals(true, extra.get("nested"));
    assertEquals("event_unknown_part_2", events.get(6).payload().get("downstreamEventId"));
  }

  @Test
  void mapsResponsesErrorEventToExecutionFailed() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"error","event_id":"event_error_1","code":"rate_limited","message":"slow down","param":null,"sequence_number":1}

                data:{"type":"response.failed","event_id":"event_failed_1","response":{"id":"resp_failed","status":"failed","error":{"type":"server_error","code":"server_error","message":"boom"}},"sequence_number":2}

                data:{"type":"response.incomplete","event_id":"event_incomplete_1","response":{"id":"resp_incomplete","status":"incomplete","error":null,"incomplete_details":{"reason":"max_tokens"}},"sequence_number":3}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(3, events.size());
    assertEquals(EventType.EXECUTION_FAILED, events.getFirst().eventType());
    assertEquals("error", events.getFirst().payload().get("type"));
    assertEquals("slow down", events.getFirst().payload().get("message"));
    assertEquals("event_error_1", events.getFirst().payload().get("downstreamEventId"));
    assertEquals(1L, events.getFirst().payload().get("downstreamSequenceNumber"));
    assertEquals(EventType.EXECUTION_FAILED, events.get(1).eventType());
    assertEquals("response.failed", events.get(1).payload().get("type"));
    assertEquals("response.failed", events.get(1).payload().get("downstreamEventType"));
    assertEquals("resp_failed", events.get(1).payload().get("responseId"));
    assertEquals("failed", events.get(1).payload().get("downstreamStatus"));
    assertEquals("event_failed_1", events.get(1).payload().get("downstreamEventId"));
    assertEquals(2L, events.get(1).payload().get("downstreamSequenceNumber"));
    assertEquals("server_error", events.get(1).payload().get("code"));
    assertEquals("server_error", events.get(1).payload().get("errorType"));
    assertEquals("boom", events.get(1).payload().get("message"));
    assertEquals(EventType.EXECUTION_FAILED, events.get(2).eventType());
    assertEquals("response.incomplete", events.get(2).payload().get("type"));
    assertEquals("resp_incomplete", events.get(2).payload().get("responseId"));
    assertEquals("incomplete", events.get(2).payload().get("downstreamStatus"));
    assertEquals("event_incomplete_1", events.get(2).payload().get("downstreamEventId"));
    assertEquals(3L, events.get(2).payload().get("downstreamSequenceNumber"));
    assertEquals("max_tokens", events.get(2).payload().get("incompleteReason"));
    assertEquals("max_tokens", events.get(2).payload().get("message"));
  }

  @Test
  void mapsDownstreamErrorToExecutionFailed() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"error":{"type":"invalid_request_error","code":"bad_request","message":"bad input"}}

                """));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    assertEquals(EventType.EXECUTION_FAILED, events.get(0).eventType());
    assertEquals("bad_request", events.get(0).payload().get("code"));
    assertEquals("bad input", events.get(0).payload().get("message"));
  }

  @Test
  void rejectsInvalidJsonData() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:not-json\n\n"));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CompletionException error =
        assertThrows(CompletionException.class, () -> collect(execution.events(), 1));

    assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
  }

  @Test
  void rejectsOverlongFrame() throws Exception {
    String content = "x".repeat(1100);
    RecordingClient client =
        new RecordingClient(
            completedStream(
                "data:{\"choices\":[{\"delta\":{\"content\":\"" + content + "\"}}]}\n\n"));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(1024), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CompletionException error =
        assertThrows(CompletionException.class, () -> collect(execution.events(), 1));

    assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("maxFrameBytes"));
  }

  @Test
  void timesOutWhenDownstreamOpenDoesNotComplete() {
    RecordingClient client = new RecordingClient(new CompletableFuture<>());
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(config(4096, Duration.ofMillis(10)), client);

    ExecutionException error =
        assertThrows(
            ExecutionException.class,
            () ->
                adapter
                    .execute(command(), context())
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS));

    OpenAiCompatibleAdapterException cause =
        assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CONNECT_FAILED, cause.errorCode());
    assertTrue(cause.getMessage().contains("openTimeout"));
  }

  @Test
  void timesOutWhenFirstGatewayEventDoesNotArrive() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new OpenAiCompatibleStream("resp-1", new NeverByteChunkPublisher())));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(4096, Duration.ofSeconds(1), Duration.ofMillis(10)), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CollectingSubscriber subscriber = new CollectingSubscriber(1);
    execution.events().subscribe(subscriber);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> subscriber.result.get(1, TimeUnit.SECONDS));

    OpenAiCompatibleAdapterException cause =
        assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("firstEventTimeout"));
  }

  @Test
  void timesOutWhenGatewayEventsBecomeIdle() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new OpenAiCompatibleStream(
                    "resp-1",
                    new OneChunkThenNeverByteChunkPublisher(
                        "data:{\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(4096, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofMillis(10)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CollectingSubscriber subscriber = new CollectingSubscriber(2);
    execution.events().subscribe(subscriber);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> subscriber.result.get(1, TimeUnit.SECONDS));

    assertEquals(1, subscriber.events.size());
    assertEquals("hello", subscriber.events.getFirst().payload().get("content"));
    OpenAiCompatibleAdapterException cause =
        assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_IDLE_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("idleEventTimeout"));
  }

  @Test
  void timesOutWhenTotalExecutionTimeElapses() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new OpenAiCompatibleStream(
                    "resp-1",
                    new OneChunkThenNeverByteChunkPublisher(
                        "data:{\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(
                4096,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CollectingSubscriber subscriber = new CollectingSubscriber(2);
    execution.events().subscribe(subscriber);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> subscriber.result.get(1, TimeUnit.SECONDS));

    assertEquals(1, subscriber.events.size());
    assertEquals("hello", subscriber.events.getFirst().payload().get("content"));
    OpenAiCompatibleAdapterException cause =
        assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("totalExecutionTimeout"));
  }

  @Test
  void delegatesCancelToDownstreamClient() throws Exception {
    RecordingClient client = new RecordingClient(completedStream(""));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);
    ExecutionHandle handle =
        new ExecutionHandle(
            "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of());

    DownstreamExecutionState state =
        adapter.cancel(handle, context()).toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(DownstreamExecutionState.CANCELED, state);
    assertEquals(handle, client.canceledHandle);
  }

  @Test
  void timesOutWhenDownstreamCancelDoesNotComplete() {
    RecordingClient client = new RecordingClient(completedStream(""), new CompletableFuture<>());
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(
                4096,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10)),
            client);
    ExecutionHandle handle =
        new ExecutionHandle(
            "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of());

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> adapter.cancel(handle, context()).toCompletableFuture().join());

    assertEquals(handle, client.canceledHandle);
    OpenAiCompatibleAdapterException cause =
        assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("cancelTimeout"));
  }

  @Test
  void delegatesSubmitInputToDownstreamClientWhenEndpointIsConfigured() throws Exception {
    RecordingClient client = new RecordingClient(completedStream(""));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            configWithSubmitInputEndpoint(
                "https://models.internal.example/v1/responses/{downstreamRunId}/input"),
            client);
    SubmitInputCommand command =
        new SubmitInputCommand(
            new RunId("run_1"),
            new TenantId("tenant_1"),
            Map.of("approvalRequestId", "appr_1", "approved", true));
    ExecutionHandle handle =
        new ExecutionHandle(
            "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of());

    adapter.submitInput(command, handle, context()).toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(command, client.submittedCommand);
    assertEquals(handle, client.submittedHandle);
  }

  @Test
  void delegatesResumeToDownstreamClientWhenEndpointIsConfigured() throws Exception {
    RecordingClient client = new RecordingClient(completedStream(""));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            configWithResumeEndpoint(
                "https://models.internal.example/v1/responses/{downstreamRunId}/resume"),
            client);
    ExecutionHandle handle =
        new ExecutionHandle(
            "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of());

    adapter.resume(handle, "evt_9", context()).toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(handle, client.resumedHandle);
    assertEquals("evt_9", client.resumeCursor);
  }

  @Test
  void rejectsSubmitInputWhenEndpointIsNotConfigured() {
    RecordingClient client = new RecordingClient(completedStream(""));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);
    SubmitInputCommand command =
        new SubmitInputCommand(new RunId("run_1"), new TenantId("tenant_1"), Map.of());
    ExecutionHandle handle =
        new ExecutionHandle(
            "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of());

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> adapter.submitInput(command, handle, context()).toCompletableFuture().join());

    assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("does not support human input"));
  }

  @Test
  void rejectsResumeWhenNotSupported() {
    RecordingClient client = new RecordingClient(completedStream(""));
    OpenAiCompatibleAdapter adapter = new OpenAiCompatibleAdapter(config(4096), client);
    ExecutionHandle handle =
        new ExecutionHandle(
            "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of());

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> adapter.resume(handle, "evt_1", context()).toCompletableFuture().join());

    assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("does not support resume"));
  }

  private static void assertBuiltinToolEvent(
      GatewayEvent event,
      EventType eventType,
      String toolCallId,
      String toolType,
      String status,
      int index) {
    assertEquals(eventType, event.eventType());
    assertEquals(toolCallId, event.payload().get("toolCallId"));
    assertEquals(toolType, event.payload().get("toolType"));
    assertEquals(status, event.payload().get("status"));
    assertEquals(index, event.payload().get("index"));
    assertEquals(toolCallId, event.sourceEventId());
  }

  private static void assertResponseEventMetadata(
      GatewayEvent event,
      String downstreamEventType,
      String downstreamEventId,
      long sequenceNumber) {
    assertEquals(downstreamEventType, event.payload().get("downstreamEventType"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals(downstreamEventId, event.payload().get("downstreamEventId"));
    assertEquals(sequenceNumber, event.payload().get("downstreamSequenceNumber"));
  }

  private static void assertReasoningSummaryEvent(
      GatewayEvent event,
      String extensionType,
      String content,
      int summaryIndex,
      String downstreamEventType,
      String partType,
      String downstreamEventId,
      long sequenceNumber) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals(extensionType, event.payload().get("extensionType"));
    assertEquals(content, event.payload().get("content"));
    assertEquals("rs_1", event.payload().get("itemId"));
    assertEquals(0, event.payload().get("outputIndex"));
    assertEquals(summaryIndex, event.payload().get("summaryIndex"));
    assertEquals(downstreamEventType, event.payload().get("downstreamEventType"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals(downstreamEventId, event.payload().get("downstreamEventId"));
    assertEquals(sequenceNumber, event.payload().get("downstreamSequenceNumber"));
    if (partType.isBlank()) {
      assertFalse(event.payload().containsKey("partType"));
    } else {
      assertEquals(partType, event.payload().get("partType"));
    }
    assertEquals("rs_1", event.sourceEventId());
  }

  private static void assertReasoningTextEvent(
      GatewayEvent event,
      String extensionType,
      String content,
      String downstreamEventType,
      String downstreamEventId,
      long sequenceNumber) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals(extensionType, event.payload().get("extensionType"));
    assertEquals(content, event.payload().get("content"));
    assertEquals("rs_1", event.payload().get("itemId"));
    assertEquals(0, event.payload().get("outputIndex"));
    assertEquals(0, event.payload().get("contentIndex"));
    assertEquals(downstreamEventType, event.payload().get("downstreamEventType"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals(downstreamEventId, event.payload().get("downstreamEventId"));
    assertEquals(sequenceNumber, event.payload().get("downstreamSequenceNumber"));
    assertEquals("rs_1", event.sourceEventId());
  }

  private static void assertOutputItemExtension(
      GatewayEvent event,
      String extensionType,
      String itemId,
      String itemType,
      String status,
      int index) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals(extensionType, event.payload().get("extensionType"));
    assertEquals(itemId, event.payload().get("itemId"));
    assertEquals(itemType, event.payload().get("itemType"));
    assertEquals(status, event.payload().get("status"));
    assertEquals(index, event.payload().get("index"));
    assertEquals("assistant", event.payload().get("role"));
    assertEquals(itemId, event.sourceEventId());
  }

  private static void assertCodeInterpreterCodeEvent(
      GatewayEvent event, String extensionType, String code, int index) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals(extensionType, event.payload().get("extensionType"));
    assertEquals("ci_1", event.payload().get("toolCallId"));
    assertEquals("code_interpreter", event.payload().get("toolType"));
    assertEquals(code, event.payload().get("code"));
    assertEquals(index, event.payload().get("index"));
    assertEquals("ci_1", event.sourceEventId());
  }

  private static void assertMcpArgumentsDoneEvent(
      GatewayEvent event,
      String arguments,
      int index,
      String downstreamEventId,
      long sequenceNumber) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals("mcp.arguments.done", event.payload().get("extensionType"));
    assertEquals("mcp_1", event.payload().get("toolCallId"));
    assertEquals("mcp", event.payload().get("toolType"));
    assertEquals(arguments, event.payload().get("arguments"));
    assertEquals(index, event.payload().get("index"));
    assertEquals("response.mcp_call_arguments.done", event.payload().get("downstreamEventType"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals(downstreamEventId, event.payload().get("downstreamEventId"));
    assertEquals(sequenceNumber, event.payload().get("downstreamSequenceNumber"));
    assertEquals("mcp_1", event.sourceEventId());
  }

  private static void assertCustomToolInputDoneEvent(
      GatewayEvent event, String input, int index, String downstreamEventId, long sequenceNumber) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals("custom_tool.input.done", event.payload().get("extensionType"));
    assertEquals("ctc_1", event.payload().get("toolCallId"));
    assertEquals("custom_tool", event.payload().get("toolType"));
    assertEquals(input, event.payload().get("input"));
    assertEquals(index, event.payload().get("index"));
    assertEquals(
        "response.custom_tool_call_input.done", event.payload().get("downstreamEventType"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals(downstreamEventId, event.payload().get("downstreamEventId"));
    assertEquals(sequenceNumber, event.payload().get("downstreamSequenceNumber"));
    assertEquals("ctc_1", event.sourceEventId());
  }

  private static void assertOutputAudioTranscriptEvent(
      GatewayEvent event,
      String extensionType,
      String content,
      String downstreamEventId,
      long sequenceNumber) {
    assertEquals(EventType.EXTENSION, event.eventType());
    assertEquals(extensionType, event.payload().get("extensionType"));
    assertEquals(content, event.payload().get("content"));
    assertEquals("aud_1", event.payload().get("itemId"));
    assertEquals(7, event.payload().get("outputIndex"));
    assertEquals(2, event.payload().get("contentIndex"));
    assertEquals("resp_1", event.payload().get("responseId"));
    assertEquals(downstreamEventId, event.payload().get("downstreamEventId"));
    assertEquals(sequenceNumber, event.payload().get("downstreamSequenceNumber"));
    assertEquals("aud_1", event.sourceEventId());
  }

  private static OpenAiCompatibleAdapterConfig config(int maxFrameBytes) {
    return config(maxFrameBytes, Duration.ofSeconds(1));
  }

  private static OpenAiCompatibleAdapterConfig config(int maxFrameBytes, Duration timeout) {
    return new OpenAiCompatibleAdapterConfig(
        service(), maxFrameBytes, timeout, "gpt-4o-mini", "Authorization", "token-1");
  }

  private static OpenAiCompatibleAdapterConfig config(
      int maxFrameBytes, Duration openTimeout, Duration firstEventTimeout) {
    return new OpenAiCompatibleAdapterConfig(
        service(),
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        firstEventTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        "gpt-4o-mini",
        "Authorization",
        "token-1",
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        null,
        null);
  }

  private static OpenAiCompatibleAdapterConfig configWithSubmitInputEndpoint(
      String submitInputEndpointTemplate) {
    return new OpenAiCompatibleAdapterConfig(
        service(),
        4096,
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        "gpt-4o-mini",
        "Authorization",
        "token-1",
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        submitInputEndpointTemplate,
        null);
  }

  private static OpenAiCompatibleAdapterConfig configWithResumeEndpoint(
      String resumeEndpointTemplate) {
    return new OpenAiCompatibleAdapterConfig(
        service(),
        4096,
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        "gpt-4o-mini",
        "Authorization",
        "token-1",
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        null,
        resumeEndpointTemplate);
  }

  private static OpenAiCompatibleAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout) {
    return new OpenAiCompatibleAdapterConfig(
        service(),
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        DEFAULT_TOTAL_EXECUTION_TIMEOUT,
        DEFAULT_CANCEL_TIMEOUT,
        "gpt-4o-mini",
        "Authorization",
        "token-1",
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        null,
        null);
  }

  private static OpenAiCompatibleAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout) {
    return config(
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        DEFAULT_CANCEL_TIMEOUT);
  }

  private static OpenAiCompatibleAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout,
      Duration cancelTimeout) {
    return new OpenAiCompatibleAdapterConfig(
        service(),
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        cancelTimeout,
        "gpt-4o-mini",
        "Authorization",
        "token-1",
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        null,
        null,
        null,
        null);
  }

  private static ServiceDescriptor service() {
    return service(true);
  }

  private static ServiceDescriptor service(boolean multimodalInput) {
    return service(
        URI.create("https://models.internal.example/v1/chat/completions"), multimodalInput);
  }

  private static ServiceDescriptor service(URI endpoint, boolean multimodalInput) {
    return new ServiceDescriptor(
        "model-chat",
        ServiceType.MODEL,
        endpoint,
        OpenAiCompatibleAdapter.ADAPTER_ID,
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, false, false, multimodalInput, true),
        1);
  }

  private static CreateRunCommand command() {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        "model-chat",
        "idem-1",
        List.of(new InputPart(InputPartType.TEXT, "hello", null)),
        Map.of());
  }

  private static CreateRunCommand multimodalCommand() {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        "model-chat",
        "idem-1",
        List.of(
            new InputPart(InputPartType.TEXT, "describe", null),
            new InputPart(
                InputPartType.IMAGE_REF, "https://objects.internal/img-1.png", "image/png")),
        Map.of());
  }

  private static ExecutionContext context() {
    return new ExecutionContext(new RunId("run_1"), new TenantId("tenant_1"), "trace-1", Map.of());
  }

  private static CompletionStage<OpenAiCompatibleStream> completedStream(String... chunks) {
    return CompletableFuture.completedFuture(
        new OpenAiCompatibleStream("resp-1", new ByteChunkPublisher(chunks)));
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher, long request) {
    CollectingSubscriber subscriber = new CollectingSubscriber(request);
    publisher.subscribe(subscriber);
    return subscriber.result.join();
  }

  private static final class RecordingClient implements OpenAiCompatibleClient {

    private final CompletionStage<OpenAiCompatibleStream> stream;
    private final CompletionStage<Void> cancelResult;
    private ExecutionHandle canceledHandle;
    private SubmitInputCommand submittedCommand;
    private ExecutionHandle submittedHandle;
    private ExecutionHandle resumedHandle;
    private String resumeCursor;
    private OpenAiCompatibleRequest request;

    private RecordingClient(CompletionStage<OpenAiCompatibleStream> stream) {
      this(stream, CompletableFuture.completedFuture(null));
    }

    private RecordingClient(
        CompletionStage<OpenAiCompatibleStream> stream, CompletionStage<Void> cancelResult) {
      this.stream = stream;
      this.cancelResult = cancelResult;
    }

    @Override
    public CompletionStage<OpenAiCompatibleStream> open(
        CreateRunCommand request,
        OpenAiCompatibleRequest downstreamRequest,
        ExecutionContext context,
        ServiceDescriptor service,
        OpenAiCompatibleAdapterConfig config) {
      this.request = downstreamRequest;
      return stream;
    }

    @Override
    public CompletionStage<Void> cancel(ExecutionHandle handle, ExecutionContext context) {
      canceledHandle = handle;
      return cancelResult;
    }

    @Override
    public CompletionStage<Void> submitInput(
        SubmitInputCommand command,
        ExecutionHandle handle,
        ExecutionContext context,
        ServiceDescriptor service,
        OpenAiCompatibleAdapterConfig config,
        OpenAiCompatibleCredentialProvider credentialProvider) {
      submittedCommand = command;
      submittedHandle = handle;
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
        ExecutionHandle handle,
        String cursor,
        ExecutionContext context,
        ServiceDescriptor service,
        OpenAiCompatibleAdapterConfig config,
        OpenAiCompatibleCredentialProvider credentialProvider) {
      resumedHandle = handle;
      resumeCursor = cursor;
      return CompletableFuture.completedFuture(new EmptyGatewayEventPublisher());
    }
  }

  private static final class EmptyGatewayEventPublisher implements Flow.Publisher<GatewayEvent> {

    @Override
    public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long requested) {
              subscriber.onComplete();
            }

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class RecordingSecretStore implements OpenAiCompatibleSecretStore {

    private final String token;
    private String serviceId;
    private String tenantId;

    private RecordingSecretStore(String token) {
      this.token = token;
    }

    @Override
    public java.util.Optional<String> bearerToken(
        ServiceDescriptor service, ExecutionContext context) {
      serviceId = service.serviceId();
      tenantId = context.tenantId().value();
      return java.util.Optional.of(token);
    }
  }

  private static final class ByteChunkPublisher implements Flow.Publisher<byte[]> {

    private final List<byte[]> chunks;

    private ByteChunkPublisher(String... chunks) {
      this.chunks =
          List.of(chunks).stream().map(value -> value.getBytes(StandardCharsets.UTF_8)).toList();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private int index;
            private boolean canceled;

            @Override
            public void request(long requested) {
              if (requested <= 0 || canceled) {
                return;
              }
              long emitted = 0;
              while (emitted < requested && index < chunks.size() && !canceled) {
                byte[] chunk = chunks.get(index);
                index++;
                subscriber.onNext(chunk);
                emitted++;
              }
              if (index == chunks.size() && !canceled) {
                canceled = true;
                subscriber.onComplete();
              }
            }

            @Override
            public void cancel() {
              canceled = true;
            }
          });
    }
  }

  private static final class NeverByteChunkPublisher implements Flow.Publisher<byte[]> {

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long requested) {}

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class OneChunkThenNeverByteChunkPublisher implements Flow.Publisher<byte[]> {

    private final byte[] chunk;

    private OneChunkThenNeverByteChunkPublisher(String chunk) {
      this.chunk = chunk.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private boolean emitted;

            @Override
            public void request(long requested) {
              if (requested <= 0 || emitted) {
                return;
              }
              emitted = true;
              subscriber.onNext(chunk);
            }

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final long request;
    private final List<GatewayEvent> events = new ArrayList<>();
    private final CompletableFuture<List<GatewayEvent>> result = new CompletableFuture<>();

    private CollectingSubscriber(long request) {
      this.request = request;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(request);
    }

    @Override
    public void onNext(GatewayEvent item) {
      events.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
      result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      result.complete(List.copyOf(events));
    }
  }
}
