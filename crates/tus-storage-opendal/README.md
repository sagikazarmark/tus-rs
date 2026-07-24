# tus-storage-opendal

[![crates.io](https://img.shields.io/crates/v/tus-storage-opendal?style=flat-square)](https://crates.io/crates/tus-storage-opendal)
[![docs.rs](https://img.shields.io/docsrs/tus-storage-opendal?style=flat-square)](https://docs.rs/tus-storage-opendal)

**[Apache OpenDAL](https://opendal.apache.org/) storage backend for [tus-protocol](https://crates.io/crates/tus-protocol) resumable uploads.**

`tus-storage-opendal` implements the `tus_protocol::Storage` contract on top of
a caller-provided [`opendal::Operator`], so uploads can land on any OpenDAL
service: local filesystem, S3, GCS, Azure Blob, and more. Backends without
native append are handled with a per-upload staging layout instead of
read-modify-write, keeping PATCH handling linear in the number of chunks.

## Install

Enable the passthrough feature for each OpenDAL service you need:

```toml
[dependencies]
tus-storage-opendal = { version = "0.0.1", features = ["services-s3"] }
```

## Quick Start

Construct the operator from this crate's `opendal` re-export so it matches the
exact `opendal` version the crate links against:

```rust,no_run
use tus_storage_opendal::{opendal, OpendalStorage};

fn storage() -> opendal::Result<OpendalStorage> {
    let builder = opendal::services::S3::default()
        .bucket("my-bucket")
        .region("us-east-1");
    let operator = opendal::Operator::new(builder)?.finish();

    Ok(OpendalStorage::new(operator).with_prefix("uploads"))
}
```

Pass the resulting storage to your `tus_protocol::Protocol` (or `tus-axum`
router) like any other backend.

## Feature Flags

Passthrough features enable the same-named `opendal` service backends, so
downstream crates do not need their own version-locked `opendal` dependency:

- `services-azblob`
- `services-fs`
- `services-gcs`
- `services-memory`
- `services-s3`

To use any other OpenDAL backend, add a direct `opendal` dependency (matching
the re-exported version) and enable its `services-*` feature there;
`OpendalStorage` works with any `opendal::Operator` regardless of how the
backend feature was enabled.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
