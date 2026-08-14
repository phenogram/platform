# Telegram Bot API image provenance

This directory builds Phenogram's Bot API data-plane image from the official
[`tdlib/telegram-bot-api`](https://github.com/tdlib/telegram-bot-api) source.

- Telegram Bot API version: `10.2`
- Telegram Bot API commit: `adfd7f6a8e990272851777eeb3ae0def4216f161`
- TDLib submodule commit: `a9966eb3704a3351568c28013fed67d797c17828`
- Upstream license: Boost Software License 1.0
- Local patch: `patches/0001-phenogram-update-tap.patch`

The local patch is intentionally limited to an opt-in, non-blocking Unix
datagram observer, its cumulative counters, and its test harness. It does not
replace the official request parser, `TQueue`, long polling, webhook delivery,
or update acknowledgement behavior. The source-order check fails the image
build if the hook moves ahead of native delivery scheduling or starts accepting
bot tokens.

Phenogram deliberately leaves the upstream server's native storage model
unchanged. In particular, upstream may use the complete bot token in its
on-disk directory layout: production state is rooted below `<raw-token>/` and
test-DC state below `<raw-token>:T/` (with upstream's filesystem fallback where
needed). Persisted runtime state is not wrapped in a Phenogram-specific
encryption or key-derivation layer. There is no `OFFICIAL_STORAGE_KEY`, hashed
token-directory mapping, custom TDLib `database_encryption_key`, or encrypted
Phenogram envelope. Access to the Bot API PVC, its snapshots, or the node
filesystem must therefore be treated as access to bot credentials.

Protocol version 1 is defined in
[`UPDATE_TAP_PROTOCOL.md`](./UPDATE_TAP_PROTOCOL.md). Ordinary type-1 update
copies are best-effort: missing, slow, or full collectors cause the copy to be
dropped and counted. Managed-bot lifecycle type-2 records are appended to a
bounded persistent observer queue only after native handling and replay until
the collector commits and ACKs them. Overflow, expiry, or persistence failure
still fails open. Neither observer mode can apply backpressure or return a
failure to the official Bot API delivery path.

The image build checks both pinned commits, requires the patch to apply cleanly,
builds the complete official server, and runs real `AF_UNIX` `SOCK_DGRAM` tests
covering framing, fragmentation on Linux, a missing collector, recovery after a
collector bind, and full-queue backpressure.
