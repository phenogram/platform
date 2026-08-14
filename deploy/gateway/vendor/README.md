# Vendored Hyper compatibility patch

`hyper/` is the unmodified crates.io source for `hyper` 1.11.0 except for two
documented server-parser compatibility changes in `src/proto/h1/role.rs`.

- Upstream: https://crates.io/crates/hyper/1.11.0
- Crates.io checksum: `d22053281f852e11534f5198498373cbb59295120a20771d90f7ed1897490a72`
- License: MIT; the upstream `LICENSE` file is preserved in `hyper/LICENSE`.
- Patches: raise the HTTP/1 request-target cap from 65,534 bytes to 262,144
  bytes, and size server header slots from the received newline count instead
  of rejecting the 101st header. Both are bounded by the 262,144-byte request
  head accepted by the pinned official Telegram Bot API server.

The gateway lockfile and `[patch.crates-io]` entry keep this source pinned. Do
not add unrelated changes to the vendored crate.
