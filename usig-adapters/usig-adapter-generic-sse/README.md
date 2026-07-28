# Generic SSE JSON Adapter

## Purpose

`usig-adapter-generic-sse` adapts trusted internal SSE services whose `data:` frames contain JSON. It is intended for simple streaming task services with stable JSON fields. Complex business workflows should use a dedicated Adapter.

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| streaming | supported | Opens downstream SSE through `DownstreamSseClient` and maps frames to `GatewayEvent`. |
| cancel | supported | Delegates cancellation to `DownstreamSseClient.cancel`. |
| resume | not supported | Returns an explicit unsupported Adapter error; callers must rely on Gateway replay only. |
| replay | gateway-owned | Replay is handled by `RunEventPublisher`/`ReplayStore`, not by this Adapter. |
| taskQuery | limited | `queryState` currently returns `RUNNING` as a local boundary. |
| humanInput | not supported | `submitInput` fails explicitly. |
| multimodalInput | not supported | Generic JSON mapping does not transform image/file content. |
| structuredOutput | supported by mapping | Structured fields can be projected through configured `FieldMapping`. |
| retrySafety | NONE | Downstream create/resume retry safety is not declared for this generic adapter, so the gateway will not retry create execution for it. |

## Contract

- Downstream endpoints must come from `ServiceDescriptor`; this Adapter must not accept arbitrary client-provided URLs.
- SSE frames are decoded with a configured `maxFrameBytes` limit.
- Downstream open timeout, first mapped event timeout, idle event timeout, total execution timeout and cancel timeout are configured independently.
- Timeout failures expose stable gateway `ErrorCode` values for connect, first event, idle event, total execution and cancel timeout paths.
- Only configured downstream event types can be mapped.
- Output to the gateway is always `GatewayEvent`; raw downstream frames are not exposed to clients.
- `sourceEventId` is preserved from mapping configuration when available.

## Tests

Primary contract coverage lives in `GenericSseJsonAdapterContractTest` and covers normal mapping, chunked frames, invalid JSON, overlong frames, downstream interruption, open timeout, first mapped event timeout, idle event timeout, total execution timeout, cancel delegation, cancel timeout and explicit resume rejection.
