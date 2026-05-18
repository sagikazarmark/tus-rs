# AGENTS.md

## Conventions

- Structure Rust source files with the primary public trait or struct first, then impls, then supporting types, then internal helpers, then tests.
- Prefer `foo.rs` + `foo/` over `foo/mod.rs`.
- Keep dependency entries in `Cargo.toml` alphabetized within each table.
- Enable only the required, minimal Cargo features for downstream dependencies, especially normal dependencies.
- Prefer fine-grained features over umbrella features like `native` or `full-native` when only specific backends or extensions are needed.
- Treat default-feature changes that affect public behavior, such as default TLS/HTTPS support, as explicit behavior changes rather than incidental cleanup.
- Group multiple imports from the same external or workspace crate in a single `use crate_name::{...};` tree when practical.
- Prefer root re-exports for public `tus_protocol` APIs, such as `tus_protocol::Storage` or `tus_protocol::UploadState`. Use deeper paths for concrete feature-gated backend types that are not re-exported, such as `tus_protocol::storage::memory::MemoryStorage`.

## Developer environment

The project uses [devenv](https://devenv.sh/) as the developer environment.
