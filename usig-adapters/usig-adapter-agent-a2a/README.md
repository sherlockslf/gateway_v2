# Agent A2A Adapter

## Purpose

`usig-adapter-agent-a2a` adapts an A2A-style agent service stream into the USIG `GatewayEvent` contract. It is a dedicated Agent Adapter boundary for task, message, artifact, status, user-input and approval semantics.

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| streaming | supported | Parses downstream SSE `data:` JSON frames. |
| cancel | supported when client implementation supports it | Adapter delegates cancellation to `AgentA2aClient`. |
| resume | supported when client implementation supports it | Adapter delegates resume to `AgentA2aClient` using the stored downstream task id and gateway cursor. |
| replay | gateway-owned | Replay remains handled by `RunEventPublisher`/`ReplayStore`. |
| taskQuery | supported when client implementation supports it | Adapter delegates status lookup and maps status text to `DownstreamExecutionState`. |
| humanInput | supported when client implementation supports it | Adapter delegates supplemental input payloads to the downstream client. |
| multimodalInput | service capability owned | This Adapter does not bypass application-level multimodal validation. |
| structuredOutput | not specialized | Structured payloads are preserved only through supported event payload fields. |
| retrySafety | NONE | Product idempotency and source cursor semantics must be verified before enabling safe retries. |

## Contract

- Adapter output is always `GatewayEvent`; downstream raw SSE frames are not exposed to clients.
- Timeout failures expose stable gateway `ErrorCode` values for open, first event, idle event, total execution and cancel timeout paths.
- `task.status` maps started/running/waiting/completed/failed/canceled states to stable execution/input events.
- `message.part` maps text parts to `TEXT_DELTA`; non-text parts are preserved as `ARTIFACT_UPDATED`.
- `artifact.created`, `artifact.updated` and `artifact.completed` map to the corresponding artifact lifecycle events.
- `input.required` maps to `INPUT_REQUIRED`, and `approval.required` maps to `APPROVAL_REQUIRED`.
- Unknown downstream event types are preserved as `EXTENSION` with `extensionType=agent.a2a.raw`.
- Invalid JSON, invalid UTF-8 and overlong SSE frames fail explicitly.
- This module is a local protocol boundary and contract-test target. It must not be counted as a real business system integration until wired to a concrete product Agent service and validated against that service.

## Tests

Primary contract coverage lives in `AgentA2aAdapterContractTest`. The tests cover task lifecycle mapping, message parts, artifact lifecycle, input/approval events, unknown event fallback, chunked SSE frames, invalid JSON, overlong frames, downstream open timeout, first event timeout, idle event timeout, total execution timeout, cancel timeout, queryState, submitInput and resume delegation.
