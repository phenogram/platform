# Phenogram update tap protocol

The Phenogram Bot API image is the official `telegram-bot-api` 10.2 source at
commit `adfd7f6a8e990272851777eeb3ae0def4216f161` with one isolated observer
patch. The observer is disabled unless the server receives
`--update-tap-socket=PATH`.

The official `TQueue`, long polling, and `WebhookActor` remain the canonical
delivery path. For a type 1 frame, after a successful `TQueue::push`, the server
first performs the native long-poll wakeup or enqueues the native webhook
wakeup. Only then does it make a best-effort `sendmsg` to `PATH` using an
unconnected, non-blocking `AF_UNIX` `SOCK_DGRAM` socket.

Managed-child discovery uses a separate type 2 lifecycle frame. The official
`add_update(UpdateType::ManagedBot, ...)` call runs first, including its native
allowed-update filtering, queueing, and wakeup. Only after that call returns,
the patch appends a 33-byte token-free record to reserved persistent TQueue
`-0x50475554` and attempts a non-blocking emit. This allows discovery even when
a bot excludes `managed_bot` from `allowed_updates`, without changing what
Telegram delivers to the bot owner.

Type 1 remains pure best-effort: there is no acknowledgement or retry. Type 2
is replayed head-first once per second until the collector commits the
idempotent receipt and managed-job mutation, then returns an exact ACK on the
private Unix socket. A lost frame, lost ACK, collector restart, or PostgreSQL
outage therefore causes replay, never native-delivery backpressure.

The lifecycle queue is capped at 10,000 records and each record expires after
seven days. A full queue, local persistence rejection, corrupt record, or
expiry fails open and increments critical counters. No type 1 or type 2 path
contains a bot token, performs a network request, or waits for PostgreSQL or the
collector. Ordinary Bot API polling/webhook delivery never consults this
observer queue.

## Transport limits

- Maximum datagram: 32,768 bytes.
- Fixed header: 64 bytes.
- Maximum fragment payload: 32,704 bytes.
- Maximum queued update payload: 262,144 bytes.
- Maximum fragments per event: 9.
- Every integer is unsigned and encoded in network byte order (big-endian).
- Fragments are emitted in ascending index order with contiguous offsets.
- One failed fragment abandons the remaining fragments for that copied event.
  A collector must expire and discard incomplete assemblies.

## Version 1 update frame

| Offset | Size | Field | Value |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `PGUT` |
| 4 | 1 | version | `1` |
| 5 | 1 | frame type | `1` (`update`) or `2` (`managed_bot_lifecycle`) |
| 6 | 1 | flags | bit 0: test data center; all other bits must be zero |
| 7 | 1 | header length | `64` |
| 8 | 8 | producer instance ID | Non-cryptographic per-process nonce |
| 16 | 8 | event sequence | Per-process counter starting at 1 |
| 24 | 8 | bot ID | Numeric Telegram bot user ID |
| 32 | 4 | event ID | Native `update_id` for type 1; persistent observer queue ID for type 2 |
| 36 | 4 | expiry | Unix timestamp in seconds used by the canonical `TQueue` event |
| 40 | 4 | total payload length | Length before fragmentation |
| 44 | 4 | fragment offset | Byte offset in the complete payload |
| 48 | 2 | fragment index | Zero-based |
| 50 | 2 | fragment count | One through nine |
| 52 | 4 | fragment length | Datagram length minus 64 |
| 56 | 8 | reserved / delivery nonce | Zero for type 1; positive persistent-delivery nonce for type 2 |
| 64 | variable | fragment payload | Exact bytes from the canonical `TQueue` event |

The payload is Telegram's queued JSON member sequence, for example
`"message":{"message_id":42,...}`. It deliberately does not duplicate the
outer object or `update_id`. After complete assembly, the collector reconstructs
the canonical update as:

```text
{"update_id":<official update ID>,<assembled payload>}
```

The assembly key is `(producer instance ID, event sequence)`. A collector must
also require identical bot ID, update ID, expiry, flags, total length, and
fragment count across every fragment; exact non-overlapping offsets; and all
reserved bits set to zero.

## Managed bot lifecycle frame

A type 2 frame is always one fragment. Header fields have these additional
constraints:

- bot ID is the numeric parent/manager bot ID;
- event ID is the positive reserved-TQueue event ID;
- expiry is the observer record's seven-day Unix expiry;
- total payload length and fragment length are 16;
- fragment offset and index are zero;
- fragment count is one.
- the reserved field is a positive delivery nonce persisted inside the record.

Its payload consists of two unsigned big-endian integers:

| Payload offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Owner/creator Telegram user ID |
| 8 | 8 | Child bot ID |

The first value is hierarchy metadata. The child bot ID in the second value is
the `user_id` argument needed for the parent bot's `getManagedBotToken` call. If
native `managed_bot` delivery is enabled, the collector receives both the
canonical type 1 frame and the lifecycle type 2 frame. They must converge
idempotently on one discovery/synchronization job, keyed by
`(parent bot ID, child bot ID)`.

The collector deduplicates the tuple `(pool, test DC, manager UUID, delivery
nonce)` in the same PostgreSQL transaction that creates or refreshes the child
sync job. An exact duplicate commits without changing the job generation/state
and is ACKed. An unknown parent, malformed/colliding identity, timeout, or
database error is not ACKed.

## Managed lifecycle ACK

The ACK is the exact 80-byte type 2 frame that was committed, except bytes 0-3
are ASCII `PGUA`. It therefore echoes the producer instance, wire sequence,
parent, observer event ID, expiry, delivery nonce, owner, and child. The
official server accepts an ACK only when every field matches its current queue
head and most recent successful emit. It then calls `TQueue::forget` and emits
the next head. Stale, forged, truncated, or out-of-order ACKs are ignored and
counted.

## Runtime contract

The collector owns the tap socket path: it removes a stale inode, binds
`/run/phenogram-tap/tap.sock`, requests at least a 1 MiB `SO_RCVBUF`, and sets
the socket mode to `0660`. The official server owns and binds the private
`/run/phenogram-tap/ack.sock` path at `0660`. Both containers share a `0770`
in-memory directory and group. Every tap emit is a non-blocking `sendmsg`; ACK
draining uses bounded non-blocking `recv` calls on the client scheduler.

When the statistics listener is enabled, the patch adds these cumulative
counters:

- `update_tap_sent_events`
- `update_tap_sent_datagrams`
- `update_tap_dropped_events`
- `update_tap_errors`
- `managed_lifecycle_pending`
- `managed_lifecycle_persisted`
- `managed_lifecycle_replayed`
- `managed_lifecycle_acked`
- `managed_lifecycle_overflow`
- `managed_lifecycle_persistence_errors`
- `managed_lifecycle_expired`
- `managed_lifecycle_ack_errors`

The last four lifecycle counters are critical durability-loss or protocol
signals. Production binds the official statistics listener only to
`127.0.0.1:8083`; the collector sidecar polls it asynchronously and emits an
`ERROR` signal while any critical counter is non-zero, including its increase
from the previous poll. Statistics availability never participates in pod
readiness or native Bot API request handling.

## Ordered route-drain fence

The gateway overwrites `x-phenogram-route-generation` on every admitted Bot
API request with the exact snapshot generation observed while holding the
route-table read lock. The official process counts each `Query` by numeric bot
ID and Telegram environment from construction through destruction. Its
loopback-only control action installs an in-memory fence for the exact old
token, environment, and route generation in the same `ClientManager`
serialization domain that dispatches queries. A tagged request at or below the
fence is rejected before dispatch; a later generation is allowed.

The authenticated collector helper is the only network-visible control
surface. It forwards the old token only in a loopback form body and returns
only bot ID, environment, fence generation, and active count. The gateway arms
and checks both official pools. A restarted official process has no surviving
queries, but the helper still re-arms the fence before it can return a valid
zero proof. Helper failure, malformed proof, or any non-zero count keeps the
route drain pending and never gates native Bot API readiness or request
handling.
