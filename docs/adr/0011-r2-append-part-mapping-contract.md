# R2 append maps each PATCH to one multipart part

Status: Accepted

## Context

The planned `tus-cloudflare` crate stores upload bytes in Cloudflare R2 through a
`Storage` implementation over the Workers `R2Bucket` binding. R2 has no native
append. Its only primitive for growing an object incrementally is multipart
upload, and R2's multipart is stricter than S3: per the R2 docs, every part
except the last must be **at least 5 MiB** and **all non-final parts must be the
same size**, with a maximum of 10,000 parts. The Workers binding exposes
`createMultipartUpload` / `resumeMultipartUpload` / `uploadPart` / `complete` /
`abort`, but no `uploadPartCopy` and no server-side copy, so there is no cheap
way to stitch separately-staged objects into a final object.

TUS gives the server no control over client chunk size: a PATCH may carry any
number of bytes, and a torn connection may deliver a partial chunk that TUS
would normally accept as a partial-offset advance.

## Decision

Each fully-received PATCH (and the initial Creation-With-Upload body, which
becomes the first part) becomes exactly one R2 multipart part (1:1 mapping),
streamed directly into `uploadPart`. Offset advancement is **chunk-atomic**: a
part is committed only if its whole body arrives; a torn body commits no part and
leaves the offset unchanged, so the client resumes and resends that chunk. The
R2 part number is derived deterministically from the offset, so a retry
overwrites the same part number rather than leaking a duplicate. `Upload-Offset`
therefore only ever advances on part boundaries plus the final short part, and
HEAD always reports a part-aligned resume point.

Part-size uniformity is handled by an optional constructor knob,
`part_size: Option<u64>`:

- `Some(n)` (validated `n >= 5 MiB`): every non-final PATCH must be exactly `n`,
  rejected early with `400` (which names the required size) before any bytes
  reach R2. Requires `Content-Length`. `Upload-Defer-Length` is rejected, because
  without a known total a short PATCH is ambiguous between a legal final part and
  an illegal short middle part. This is the mode a controlled client (dioxus-tus,
  the demo) points at, with `part_size` set to match the client `chunkSize`.
- `None`: no server-side size rule. Each PATCH becomes one part as-is; a
  non-uniform sequence surfaces as an error on the completing PATCH when R2's
  `complete()` rejects it ("fail late"). Unknown-length / chunked bodies and
  `Upload-Defer-Length` are tolerated, because streaming into `uploadPart` never
  buffers the body in RAM and no early size check is attempted.

An upload that completes in a single sub-threshold PATCH (or Creation-With-Upload
body) is written with a plain `put()` instead of opening a multipart upload for
one small part.

The multipart bookkeeping (R2 upload id, the part-number-to-etag list, part
size) is persisted in `StorageHandle.internal`, the `HashMap<String,String>` the
protocol already carries per upload for backend facts.

## Consequences

This backend is a conformant TUS server only for clients that obey the part-size
rule; a stock client that chunks below 5 MiB, or non-uniformly, will fail (in
`Some(n)` mode early and explicitly, in `None` mode late at completion). That is
an accepted, documented limitation, appropriate because the primary deployment
owns its frontend. At the default 5 MiB part size the 10,000-part ceiling caps a
single object at ~48.8 GiB; larger objects require a larger configured
`part_size`.

## Considered Options

**Server-side re-chunk with a durable tail (rejected for now).** The backend
owns part sizing, accumulating incoming bytes and cutting uniform 5 MiB parts
while carrying a sub-part remainder ("tail") durably between PATCHes. This is
correct for any client chunk size and any torn PATCH, i.e. a real TUS server on
R2. Rejected as the initial design because it is materially more code and state
(the tail must survive DO eviction between requests) for a correctness margin the
controlled-frontend deployment does not need. It remains the natural upgrade path
if `tus-cloudflare` later targets arbitrary third-party clients.

**Staging object per PATCH, concat on finalize (rejected).** The OpenDAL backend
writes each PATCH as its own object and concatenates on finalize. On R2 this is
strictly worse: with no `uploadPartCopy`, "concat" means re-reading every staged
object and re-uploading it, and each re-uploaded part still hits the same 5 MiB
uniform-size constraint. Multipart-with-tail dominates it.
