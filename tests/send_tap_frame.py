#!/usr/bin/env python3
"""Send one exact PGUT v1 frame sequence to a local collector socket."""

from __future__ import annotations

import json
import os
import secrets
import socket
import struct
import sys
import time


HEADER = struct.Struct(">4sBBBBQQQIIIIHHI8s")
MAX_FRAGMENT = 32_704
TEST_DC_FLAG = 1 if os.environ.get("PHENOGRAM_TAP_TEST_DC") == "1" else 0


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.environ.get(name, default))
    if not minimum <= value <= maximum:
        raise SystemExit(f"{name} must be between {minimum} and {maximum}")
    return value


def send_update(sock: socket.socket, path: str, bot_id: int, update_id: int) -> None:
    update = json.load(sys.stdin)
    if update.pop("update_id", None) != update_id or len(update) != 1:
        raise SystemExit("stdin must be one canonical Telegram Update")
    member = json.dumps(update, separators=(",", ":"), ensure_ascii=False)[1:-1].encode()
    fragments = [
        (offset, member[offset : offset + MAX_FRAGMENT])
        for offset in range(0, len(member), MAX_FRAGMENT)
    ] or [(0, b"")]
    producer = secrets.randbits(64)
    sequence = secrets.randbits(64)
    expiry = int(time.time()) + 60
    datagrams: list[bytes] = []
    for index, (offset, fragment) in enumerate(fragments):
        header = HEADER.pack(
            b"PGUT",
            1,
            1,
            TEST_DC_FLAG,
            HEADER.size,
            producer,
            sequence,
            bot_id,
            update_id,
            expiry,
            len(member),
            offset,
            index,
            len(fragments),
            len(fragment),
            b"\0" * 8,
        )
        datagrams.append(header + fragment)
    # Exercise out-of-order reassembly whenever the payload spans fragments.
    for datagram in reversed(datagrams):
        sock.sendto(datagram, path)


def send_lifecycle(
    sock: socket.socket,
    path: str,
    parent_bot_id: int,
    managed_user_id: int,
    child_bot_id: int,
) -> None:
    payload = struct.pack(">QQ", managed_user_id, child_bot_id)
    producer = env_int(
        "PHENOGRAM_TAP_PRODUCER_ID", secrets.randbits(64) or 1, 1, 2**64 - 1
    )
    sequence = env_int(
        "PHENOGRAM_TAP_EVENT_SEQUENCE", secrets.randbits(64) or 1, 1, 2**64 - 1
    )
    observer_event_id = env_int(
        "PHENOGRAM_TAP_OBSERVER_EVENT_ID",
        secrets.randbelow(1_999_999_999) + 1,
        1,
        1_999_999_999,
    )
    expiry = env_int(
        "PHENOGRAM_TAP_EVENT_EXPIRY",
        int(time.time()) + 7 * 86_400,
        int(time.time()) + 1,
        2**32 - 1,
    )
    delivery_nonce = env_int(
        "PHENOGRAM_TAP_DELIVERY_NONCE", secrets.randbits(63) or 1, 1, 2**63 - 1
    )
    header = HEADER.pack(
        b"PGUT",
        1,
        2,
        TEST_DC_FLAG,
        HEADER.size,
        producer,
        sequence,
        parent_bot_id,
        observer_event_id,
        expiry,
        len(payload),
        0,
        0,
        1,
        len(payload),
        delivery_nonce.to_bytes(8, "big"),
    )
    sock.sendto(header + payload, path)


def listen_acks(path: str, count: int) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        sock.bind(path)
        os.chmod(path, 0o660)
        sock.settimeout(10)
        try:
            for _ in range(count):
                frame = sock.recv(HEADER.size + 17)
                if len(frame) != HEADER.size + 16:
                    raise SystemExit("invalid lifecycle ACK length")
                fields = HEADER.unpack(frame[: HEADER.size])
                if fields[0] != b"PGUA" or fields[1:5] != (1, 2, TEST_DC_FLAG, HEADER.size):
                    raise SystemExit("invalid lifecycle ACK envelope")
                owner_id, child_id = struct.unpack(">QQ", frame[HEADER.size:])
                print(
                    json.dumps(
                        {
                            "producer": fields[5],
                            "sequence": fields[6],
                            "parent_bot_id": fields[7],
                            "observer_event_id": fields[8],
                            "expiry": fields[9],
                            "delivery_nonce": int.from_bytes(fields[15], "big"),
                            "owner_id": owner_id,
                            "child_id": child_id,
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
        finally:
            os.unlink(path)


def main() -> None:
    if len(sys.argv) == 4 and sys.argv[2] == "ack-listen":
        listen_acks(sys.argv[1], int(sys.argv[3]))
        return
    if len(sys.argv) < 5:
        raise SystemExit(
            "usage: send_tap_frame.py SOCKET update BOT_ID UPDATE_ID | "
            "lifecycle PARENT_BOT_ID MANAGED_USER_ID CHILD_BOT_ID | "
            "SOCKET ack-listen COUNT"
        )
    path, frame_type = sys.argv[1:3]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        if frame_type == "update" and len(sys.argv) == 5:
            send_update(sock, path, int(sys.argv[3]), int(sys.argv[4]))
        elif frame_type == "lifecycle" and len(sys.argv) == 6:
            send_lifecycle(
                sock,
                path,
                int(sys.argv[3]),
                int(sys.argv[4]),
                int(sys.argv[5]),
            )
        else:
            raise SystemExit("invalid tap frame arguments")


if __name__ == "__main__":
    main()
