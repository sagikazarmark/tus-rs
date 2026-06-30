# tus-cli

[![crates.io](https://img.shields.io/crates/v/tus-cli?style=flat-square)](https://crates.io/crates/tus-cli)
[![docs.rs](https://img.shields.io/docsrs/tus-cli?style=flat-square)](https://docs.rs/tus-cli)

**Command-line client for the [tus resumable upload protocol](https://tus.io/).**

`tus-cli` provides the `tus` binary for uploading files to tus-compatible
servers. It is built on [`tus-client`](https://crates.io/crates/tus-client) and
supports creating uploads, resuming existing upload resources, inspecting upload
state, terminating uploads, metadata, bearer-token authentication, and config
files.

## Install

Install the `tus` binary from crates.io:

```bash
cargo install tus-cli
```

Or run it directly from this repository:

```bash
cargo run -p tus-cli -- --help
```

## Quick Start

Create and upload a file at a tus collection endpoint:

```bash
tus --endpoint http://127.0.0.1:8080/files \
    upload ./hello.txt \
    --metadata filename=hello.txt
```

Upload completion is reported on stderr by default; pass `--output url` for a
stdout-only URL. Progress is shown on stderr when stderr is an interactive
terminal.

Create an empty upload URL first, then upload to it later:

```bash
tus --endpoint http://127.0.0.1:8080/files create ./hello.txt
tus upload ./hello.txt http://127.0.0.1:8080/files/<id>
```

`create` prints the created upload URL to stdout by default.

Inspect the upload:

```bash
tus info http://127.0.0.1:8080/files/<id>
```

Print machine-readable upload information:

```bash
tus info --output json http://127.0.0.1:8080/files/<id>
```

Resume or finish an existing upload by passing its upload URL after the file:

```bash
tus upload ./hello.txt http://127.0.0.1:8080/files/<id>
```

Terminate an upload when the server supports the termination extension:

```bash
tus terminate http://127.0.0.1:8080/files/<id>
```

## Commands

| Command | Purpose |
|---------|---------|
| `tus create <FILE>` | Create a new upload URL using the file length without uploading file contents. |
| `tus create --length <SIZE>` | Create a new upload URL with an explicit upload length. |
| `tus upload <FILE>` | Create a new upload at the configured collection endpoint and upload the file. |
| `tus upload <FILE> <UPLOAD_URL>` | Upload the file to an existing upload resource, resuming from the remote offset. |
| `tus info <UPLOAD_URL>` | Print the upload URL, current offset, length, and metadata. |
| `tus info --output json <UPLOAD_URL>` | Print upload information as JSON. |
| `tus terminate <UPLOAD_URL>` | Delete or cancel an upload resource. |

`UPLOAD_URL` may be an absolute URL. When `--endpoint` is configured, it may
also be an upload ID relative to that endpoint or an absolute path on the same
origin.

Upload metadata is accepted in `KEY=VALUE` form and can be repeated on `create`
and new-upload `upload` commands:

```bash
tus --endpoint http://127.0.0.1:8080/files \
    upload ./photo.jpg \
    --metadata filename=photo.jpg \
    --metadata content-type=image/jpeg
```

Metadata is only used when creating a new upload URL. It cannot be combined with
an existing upload URL.

Explicit lengths accept bare bytes or standard byte-size suffixes. `KB`, `MB`,
`GB`, and `TB` use powers of 1000; `KiB`, `MiB`, `GiB`, and `TiB` use powers of
1024.

```bash
tus --endpoint http://127.0.0.1:8080/files create --length 123KiB
tus --endpoint http://127.0.0.1:8080/files create --length 321KB
```

## Configuration

Global settings can be supplied with command-line flags, environment variables,
or a TOML, YAML, or JSON config file.

| Setting | Flag | Environment | Config key |
|---------|------|-------------|------------|
| Collection endpoint | `--endpoint <URL>` | `TUS_ENDPOINT` | `endpoint` |
| Bearer token | `--bearer-token <TOKEN>` | `TUS_BEARER_TOKEN` | `bearer_token` |
| Config file | `--config <PATH>` | `TUS_CONFIG` | n/a |

Example TOML config:

```toml
endpoint = "https://uploads.example.com/files"
bearer_token = "secret-token"
```

Upload-specific options are supplied on `tus upload`. `chunk_size` can also be
set in the config file as an upload default.

| Setting | Flag | Environment | Config key |
|---------|------|-------------|------------|
| PATCH chunk size | `tus upload --chunk-size <BYTES>` | `TUS_CHUNK_SIZE` | `chunk_size` |
| Disable progress | `tus upload --no-progress` | n/a | n/a |

## Protocol Support

`tus-cli` targets tus 1.0.0 client upload workflows through `tus-client`.

| Capability | Status | Notes |
|------------|--------|-------|
| Core protocol | Supported | Uses `POST`, `HEAD`, `PATCH`, and `DELETE` through `tus-client`. |
| Creation | Supported | `create` creates an empty upload URL; `upload <FILE>` creates and uploads when an endpoint is configured. |
| Resume | Supported | `upload <FILE> <UPLOAD_URL>` reads the current offset and continues from there. |
| Metadata | Supported | `--metadata KEY=VALUE` sends upload metadata during creation. |
| Termination | Supported | `terminate` deletes uploads when the server advertises termination support. |

## Runtime Notes

`tus-cli` is a native command-line tool. It uploads from a file-backed source so
large files are read in upload chunks instead of being buffered fully in memory.
Upload behavior, retry handling, chunking, resume offset validation, and URL
resolution are delegated to `tus-client`.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
