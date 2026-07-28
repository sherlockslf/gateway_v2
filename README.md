# Unified Streaming Interaction Gateway

This repository contains the Java 21 and Quarkus first-phase implementation of USIG, a unified streaming interaction gateway for models, agents, RAG, workflows, and long-running services.

## Source Documents

- `docs/requirements-clarification.md`
- `docs/architecture-design.md`
- `docs/contracts/gateway-event.schema.json`
- `docs/contracts/http-api-protocol.md`
- `docs/contracts/websocket-control-protocol.md`
- `docs/contracts/service-registry-admin-protocol.md`
- `03-工作记录.md`

## Current Baseline

The repository has moved beyond the initial skeleton. The current local baseline includes:

- REST APIs for run creation, service capability discovery, service listing, supplemental input with approval payload validation, and service-scoped image upload credentials plus upload callback confirmation with optional HMAC callback signature input.
- SSE replay, WebSocket command/control with local session TTL, close cleanup, and `command.auth.refresh` renewal, run lifecycle state tracking, cancellation, resume boundaries with RESUME_SAFE-only retry, bounded replay, redaction, application-level Pipeline stage contract boundaries with `SchemaValidationStage`, `TimestampNormalizationStage`, `SequenceStage`, `CoalescingStage`, `RollingRedactionStage`, `TenantPolicyStage`, `CriticalEventGuardStage`, `ReplayStage`, `SideEffectTapStage`, `ClientProjectionStage`, and `DeliveryMetricsStage`, concurrency limiting, circuit breaker boundaries, and observable/configurable one-shot create execution retry with bounded jitter delay for services that declare retry-safe create semantics.
- Generic SSE, Agent A2A, Business Workflow, OpenAI-compatible, and sample-agent Adapters with README contract artifacts.
- Business Workflow Adapter local protocol boundary for workflow/RAG business events including lifecycle, node progress, retrieval documents, answer deltas, tool calls, artifacts, input/approval requests, usage, extension fallback, cancel/queryState/submitInput/resume delegation, and open/first-event/idle/total/cancel timeout coverage.
- OpenAI-compatible Chat Completions and Responses request mapping, streaming event normalization, Responses lifecycle, completed usage, output item, output text, content part, function call arguments, terminal error, and unknown raw fallback metadata retention, Responses output text logprobs retention, Responses output text annotation metadata retention, Responses refusal, reasoning, custom tool input, MCP arguments/status/list-tools, file/web search, code interpreter, and image generation metadata retention, Responses output audio/transcript event mapping, Responses computer call, computer call output, shell call, and shell call output item mapping, Images streaming partial image mapping, unknown Responses content part extension fallback, unknown `response.*` raw extension fallback, multimodal image URL validation, local image upload credential/callback lifecycle record cleanup, credential injection, Secret Store abstraction with environment, Vault KV v2, and HTTP KMS decrypt boundary implementations, optional cancel/queryState/submitInput/resume endpoints, and covered Responses tool/event mappings.
- Redis infrastructure boundaries for replay, concurrency limiting, and circuit breaker state; PostgreSQL run metadata, service descriptor record mapping, service registry read adapter and snapshot source boundaries, bootstrap service-registry backend selection, disabled-by-default admin service-registry reload API boundary, disabled-by-default single-instance service-registry reload worker boundary, token-protected service-registry reload worker status query with local attempt timestamps and success/failure counters, and service-registry admin protocol contract artifact; Kafka side-event publish and local idempotent consume boundaries, local side-event outbox with manual and configurable background drain/retry boundaries, service-registry audit side-event fan-out, OpenTelemetry, and bootstrap-configurable S3-compatible object storage presigned upload, HMAC callback signature, and SigV4 delete infrastructure boundaries.
- GatewayEvent JSON Schema, HTTP API, service-registry admin, and WebSocket control contract artifacts with testkit file-contract coverage, plus Phase 5 and Phase 8 testkit/document artifacts for capacity and production benchmark preparation.

The implementation still does not claim product integration for a real identity provider/session validation system, vendor-native object storage callback formats, real cloud lifecycle policies or bucket event delivery, live KMS/Vault/S3 environment verification, config-center listening, cross-instance service hot reload, real Kafka consumer deployment verification, real PostgreSQL polling deployment verification, or production benchmark results. `03-工作记录.md` is the task ledger and should remain the source of truth for completed and remaining slices.

## Validation

Use the Maven Wrapper when available:

```bash
./mvnw verify
```

On Windows PowerShell:

```powershell
.\mvnw.cmd verify
```
