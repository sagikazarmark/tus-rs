# Context

## Glossary

### Body intake

The protocol responsibility of turning request body bytes plus body-related metadata into validated upload bytes before they are accepted for an upload.

### Expired upload reclamation

The process of removing upload data and upload state after an upload has expired. This is distinct from request-time expiration checks, which reject or hide expired uploads without necessarily deleting their stored data or state.
