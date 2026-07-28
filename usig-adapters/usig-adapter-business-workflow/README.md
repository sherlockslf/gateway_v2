# USIG Business Workflow Adapter

## Purpose

`usig-adapter-business-workflow` is a dedicated Adapter boundary for business workflow, RAG and long-running task services that already expose typed domain events instead of OpenAI-compatible chunks. It exists to satisfy the first real-business Adapter shape without moving business-specific protocol branches into the core runtime.

This module maps a business workflow task stream into stable `GatewayEvent` values for execution lifecycle, progress, text, tool, artifact, input, approval, usage and extension events.

## Capabilities

- Adapter id: `business-workflow-v1`.
- streaming: consumes a typed business workflow event stream and emits ordered `GatewayEvent` instances.
- timeout: downstream open timeout, first event timeout, idle event timeout, total execution timeout and cancel timeout are configured independently.
- cancel: delegates cancellation to the configured business workflow client with a bounded cancel timeout.
- resume: delegates resume to the configured business workflow client and maps the resumed stream through the same event mapper.
- replay: remains owned by the core application replay pipeline; this Adapter only preserves source event ids for replay stability.
- retrySafety: depends on the registered `ServiceDescriptor`; this Adapter does not claim idempotent create or resume safety by itself.
- Supports business events for workflow status, node progress, retrieval documents, answer deltas, tool calls, artifacts, input requests, approval requests, usage and unknown raw extensions.

## Contract

The Adapter accepts a `CreateRunCommand` plus `ExecutionContext`, calls a `BusinessWorkflowClient`, stores the returned workflow run id in `ExecutionHandle.downstreamTaskId`, and maps each downstream event into `GatewayEvent`.

Known mappings:

- `workflow.started|running|completed|failed|canceled` -> `execution.*`
- `node.started|node.progress|node.completed` -> `progress.updated`
- `retrieval.document` -> `artifact.updated`
- `answer.delta` -> `text.delta`
- `answer.done` -> `text.end`
- `tool.started|tool.arguments|tool.result|tool.failed|tool.completed` -> `tool.*`
- `artifact.created|artifact.updated|artifact.completed` -> `artifact.*`
- `input.required` -> `input.required`
- `approval.required` -> `approval.required`
- `usage.updated` -> `usage.updated`
- unknown events -> `extension` with `extensionType=business.workflow.raw`

The Adapter keeps downstream-private fields in `payload.downstreamEventType`, `payload.workflowRunId`, `payload.nodeId`, `payload.toolCallId`, `payload.artifactId` or `payload.rawEvent`; it does not expose a client UI projection format.

## Tests

`BusinessWorkflowAdapterContractTest` covers normal streaming event mapping, unknown event fallback, source event id preservation, cancel/query/submitInput/resume delegation, service support matching, open timeout, first event timeout, idle event timeout, total execution timeout and cancel timeout errors. The tests assert that emitted records are `GatewayEvent` values and that replay remains a core pipeline responsibility.
