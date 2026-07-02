# Planned final upload dependency expiration

Status: Accepted

## Context

Issue #60 asks what happens when a final upload references partial uploads that later expire or are reclaimed. Completed final uploads are deliverable content, but unfinished final uploads created through `concatenation-unfinished` are still plans over partial uploads.

## Decision

An unfinished final upload is a planned final upload. It is not deliverable content, does not retain referenced partial uploads, and becomes `410 Gone` when any referenced partial upload is expired or missing.

Planned final uploads are invalidated lazily when accessed or materialized. Expired upload reclamation does not cascade through final-upload references, because that would require reverse-reference discovery or mandatory upload inventory. A planned final upload's effective expiration is capped by the earliest referenced partial upload expiration so advertised availability and expiration listing do not outlive the dependencies.

Completed or materialized final uploads remain deliverable even if their referenced partial uploads later expire or are reclaimed. They continue exposing the original `Upload-Concat` composition metadata required by TUS.

## Consequences

Missing referenced partial state must be treated as a client-visible gone final-upload workflow, not as an internal server error.

Final-upload creation still rejects already expired or missing referenced partials and does not persist a planned final upload in that case.
