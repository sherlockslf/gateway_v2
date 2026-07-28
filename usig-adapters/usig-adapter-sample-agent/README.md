# Sample Agent Adapter

## Purpose

`usig-adapter-sample-agent` is a local demonstration Adapter used by bootstrap tests, smoke scripts and examples. It is not a real business integration.

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| streaming | supported | Emits deterministic sample `GatewayEvent` values. |
| cancel | supported | Returns `CANCELED` without calling an external service. |
| resume | supported as local sample | Emits deterministic sample resume events after a cursor. |
| replay | gateway-owned | Replay is handled by `RunEventPublisher`/`ReplayStore`, not by this Adapter. |
| taskQuery | supported as local sample | Returns `RUNNING`. |
| humanInput | supported as local sample | Accepts input without external side effects. |
| multimodalInput | supported as local sample | Capability is declared for bootstrap smoke coverage only. |
| structuredOutput | supported as local sample | Capability is declared for bootstrap smoke coverage only. |
| retrySafety | RESUME_SAFE | Local sample execution can be replayed/resumed deterministically for gateway wiring tests; gateway create execution and resume can retry once with bounded jitter before stream attachment. |

## Contract

- This Adapter exists to validate gateway transport, use case and lifecycle wiring.
- It must not be treated as the required real business Adapter.
- Output to the gateway is always `GatewayEvent`.
- Resume support is deterministic and local; the gateway only retries resume for services declaring `RESUME_SAFE`, and this sample does not prove product-level source cursor recovery.

## Tests

Sample behavior is covered through application and bootstrap tests, including create Run, SSE delivery, WebSocket cancel, service capability listing, input submission and application-level resume boundary tests.
