# tus-rs

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/sagikazarmark/tus-rs/ci.yaml?style=flat-square)](https://github.com/sagikazarmark/tus-rs/actions/workflows/ci.yaml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/sagikazarmark/tus-rs/badge?style=flat-square)](https://securityscorecards.dev/viewer/?uri=github.com/sagikazarmark/tus-rs)
[![crates.io](https://img.shields.io/crates/v/tus-protocol?style=flat-square)](https://crates.io/crates/tus-protocol)
[![docs.rs](https://img.shields.io/docsrs/tus-protocol?style=flat-square)](https://docs.rs/tus-protocol)

**A Rust implementation of the [TUS resumable upload protocol](https://tus.io/).**

## Features

- **Full standard TUS 1.0.0 protocol support on the native server path**
- **Extensible storage backends**
- **Pluggable state storage**
- **Distributed locking support**
- **Flexible hook system** for customization

## TUS Protocol Support

| Capability | Status | Notes |
|------------|--------|-------|
| [Core protocol](https://tus.io/protocols/resumable-upload#core-protocol) | Supported | `POST`, `HEAD`, `PATCH`, `OPTIONS`, offsets, metadata, and version negotiation. |
| [Creation](https://tus.io/protocols/resumable-upload#creation) | Supported | Create new uploads via `POST`. |
| [Creation-With-Upload](https://tus.io/protocols/resumable-upload#creation-with-upload) | Supported | Include data in the initial `POST` request. |
| [Creation-Defer-Length](https://tus.io/protocols/resumable-upload#creation) | Supported | Create uploads before the final size is known. |
| [Termination](https://tus.io/protocols/resumable-upload#termination) | Supported | Cancel/delete uploads via `DELETE`. |
| [Expiration](https://tus.io/protocols/resumable-upload#expiration) | Supported | Expiration timestamps, rejection of expired uploads, and background cleanup. |
| [Concatenation](https://tus.io/protocols/resumable-upload#concatenation) | Supported | Standard final concatenation is supported. The non-standard `concatenation-unfinished` check is separate and outside the stable protocol contract. |
| [Checksum](https://tus.io/protocols/resumable-upload#checksum) | Supported | Bodied and trailer checksums are supported. |

## Documentation

TODO

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
