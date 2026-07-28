# USIG Object Storage Infrastructure

## Purpose

`usig-infra-objectstorage` provides replaceable object storage infrastructure implementations behind application ports.

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| imageUploadCredential | supported for S3-compatible presigned PUT | `S3CompatibleImageUploadCredentialIssuer` implements `ImageUploadCredentialIssuer` and returns a SigV4 presigned PUT URL plus public object URL. |
| uploadConfirmation | supported for local issued records | The issuer can verify that a callback object URL was issued locally and still matches tenant, service, MIME type, content length, and expiry. |
| callbackVerification | supported for configured HMAC callback signature | When `callback-signing-secret` is configured, upload confirmation requires `sha256=<hex>` or raw hex HMAC-SHA256 over the gateway callback payload fields. |
| lifecycleCleanup | supported for local issued records | The issuer can remove unconfirmed local issued-record state after credential expiry and confirmed records after retention expiry. |
| lifecycleDeletion | supported for S3-compatible delete boundary | Confirmed uploads trigger a SigV4 `DELETE` object request when retention expires; failed deletes keep the local record for retry. |

## Contract

- Presigned upload requests use HTTP `PUT`.
- The signed request includes `Content-Type` and `host` in `X-Amz-SignedHeaders`.
- Object keys are scoped under `uploads/{tenantId}/{serviceId}/`.
- `objectUrl` uses the service `allowedImageHosts` value when configured; otherwise it uses the configured public base URI.
- Session tokens are included in the presigned query when configured.
- Local upload confirmation and cleanup are in-memory gateway record checks, not object storage native state verification.
- Callback signature verification is a configurable HMAC boundary for webhook-style callbacks; vendor-native callback formats and public-key verification still require environment-specific integration.
- Confirmed upload records are retained after upload credential expiry so retention expiry can trigger object deletion. Unconfirmed upload records are removed after upload credential expiry.
- Object deletion uses a S3-compatible SigV4 `DELETE` request boundary. Real cloud lifecycle policies, bucket event delivery, and deployed S3/OSS integration are still environment-specific validation work.

## Tests

Primary coverage lives in `S3CompatibleImageUploadCredentialIssuerTest`. Tests cover presigned URL structure, SigV4 query parameters, object URL host selection, session token propagation, local issued-record confirmation, HMAC callback signature acceptance/rejection, metadata mismatch rejection, expired unconfirmed record cleanup, confirmed object retention cleanup with delete delegation, delete failure retry behavior, SigV4 DELETE request construction, expiry/retention timestamps, and invalid configuration rejection.
