# Vendored HTTP URI compatibility patch

`http/` is the crates.io source for `http` 1.5.0 with two narrowly related URI
representation changes in `src/uri/mod.rs` and `src/uri/path.rs`.

- Upstream: https://crates.io/crates/http/1.5.0
- Crates.io checksum: `918d3568bebf352712bc2ef3d46a8bcf1a75b373be6539de198e9105cbbf9ce0`
- License: MIT OR Apache-2.0; upstream `LICENSE-MIT` and `LICENSE-APACHE` are
  preserved in `http/`.
- Patch: raise the URI length cap to 262,144 bytes and widen the path/query
  offset from `u16` to `u32`, so Hyper can represent any request target that
  fits the pinned official Telegram Bot API server's 262,144-byte request-head
  limit.

The gateway lockfile and `[patch.crates-io]` entry keep this source pinned. Do
not add unrelated changes to the vendored crate.
