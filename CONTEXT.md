# Context

## Glossary

### Body intake

The protocol responsibility of turning request body bytes plus body-related metadata into validated upload bytes before they are accepted for an upload.

### Expired upload reclamation

The process of removing upload data and upload state after an upload has expired. This is distinct from request-time expiration checks, which reject or hide expired uploads without necessarily deleting their stored data or state.

### Final upload materialization

The process of turning a final upload's ordered partial uploads into the upload state and upload data exposed to clients. This includes the distinction between planned final uploads that are not ready yet and final uploads whose data can be materialized from complete parts.
