#!/usr/bin/env python3
"""Tiny Telegram Bot API contract double for local end-to-end checks."""

from __future__ import annotations

import json
import os
import re
from email.parser import BytesParser
from email.policy import default as email_policy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


STATE: dict[str, object] = {
    "webhook": {},
    "test_webhook": {},
    "child_webhook": {},
    "test_child_webhook": {},
    "calls": [],
    "deliveries": [],
    "file_requests": [],
    "fail_next_logout": False,
    "fail_next_close": False,
    "rotated_child_token": False,
    "gateway_in_flight": 0,
    "gateway_official_fenced": True,
    "gateway_official_active_requests": {"standard": 0, "local": 0},
    "drain_requests": [],
    "next_message_id": 9001,
}
BOT_PATH = re.compile(r"^/bot([^/]+)/(test/)?([^/]+)$")
FILE_PATH = re.compile(r"^/file/bot([^/]+)/(test/)?(.+)$")
MANAGER_TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
TEST_MANAGER_TOKEN = "123456789:TESTABCDEFGHIJKLMNOPQRSTUVWXYZabcd"
CHILD_TOKEN = "987654321:abcdefghijklmnopqrstuvwxyzABCDEF"
TEST_CHILD_TOKEN = "987654321:TESTabcdefghijklmnopqrstuvwxyzABCD"
CHILD_ROTATED_TOKEN = "987654321:ROTATEDabcdefghijklmnopqrstuvwxyz"
CHILD_BOT_ID = 987654321

BOT_IDENTITIES = {
    "manager": {
        "id": 123456789,
        "is_bot": True,
        "first_name": "Phenogram Test",
        "username": "phenogram_test_bot",
    },
    "child": {
        "id": CHILD_BOT_ID,
        "is_bot": True,
        "first_name": "Managed E2E Child",
        "username": "managed_e2e_child_bot",
    },
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path == "/__state":
            self.respond(200, STATE)
            return
        if self.path == "/health/ready":
            # The lifecycle smoke test points the private admin origin at this
            # contract double. A very high acknowledged generation lets the
            # control plane prove both withdrawal and publication fences.
            self.respond(200, {"status": "ready", "snapshot_generation": "9223372036854775807"})
            return
        file_match = FILE_PATH.match(urlparse(self.path).path)
        if file_match:
            token, test_marker, file_path = file_match.groups()
            test_dc = bool(test_marker)
            bot_name = self.bot_name(token, test_dc)
            if bot_name is None:
                self.respond(401, {"ok": False, "error_code": 401, "description": "Unauthorized"})
                return
            data = b"phenogram-test-file\n"
            status = 200
            start = 0
            end = len(data) - 1
            requested_range = self.headers.get("Range")
            if requested_range:
                match = re.fullmatch(r"bytes=(\d+)-(\d*)", requested_range)
                if match is None:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{len(data)}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else len(data) - 1
                end = min(end, len(data) - 1)
                if start > end or start >= len(data):
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{len(data)}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                status = 206
            payload = data[start : end + 1]
            STATE["file_requests"].append(
                {
                    "bot": bot_name,
                    "credential": self.credential_label(token),
                    "test_dc": test_dc,
                    "file_path": file_path,
                    "range": requested_range,
                }
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.end_headers()
            self.wfile.write(payload)
            return
        self.telegram_request()

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/__reset":
            STATE["webhook"] = {}
            STATE["test_webhook"] = {}
            STATE["child_webhook"] = {}
            STATE["test_child_webhook"] = {}
            STATE["calls"] = []
            STATE["deliveries"] = []
            STATE["file_requests"] = []
            STATE["fail_next_logout"] = False
            STATE["fail_next_close"] = False
            STATE["rotated_child_token"] = False
            STATE["gateway_in_flight"] = 0
            STATE["gateway_official_fenced"] = True
            STATE["gateway_official_active_requests"] = {"standard": 0, "local": 0}
            STATE["drain_requests"] = []
            STATE["next_message_id"] = 9001
            self.respond(200, {"ok": True})
            return
        if path == "/__fail_next_logout":
            STATE["fail_next_logout"] = True
            self.respond(200, {"ok": True})
            return
        if path == "/__rotate_managed_token":
            STATE["rotated_child_token"] = True
            STATE["calls"] = []
            self.respond(200, {"ok": True})
            return
        if path == "/__set_gateway_in_flight":
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) if length else b"{}")
            STATE["gateway_in_flight"] = max(0, int(body.get("in_flight", 0)))
            self.respond(200, {"ok": True})
            return
        if path == "/__set_gateway_official_requests":
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) if length else b"{}")
            STATE["gateway_official_fenced"] = bool(body.get("fenced", True))
            STATE["gateway_official_active_requests"] = {
                "standard": body.get("standard", 0),
                "local": body.get("local", 0),
            }
            self.respond(200, {"ok": True})
            return
        if path == "/internal/routes/drain":
            if not self.headers.get("Authorization", "").startswith("Bearer "):
                self.respond(401, {"error": "unauthorized"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) if length else b"{}")
            if (
                body.get("schema_version") != 1
                or not str(body.get("token_lookup_hash", "")).startswith("phg_")
                or not isinstance(body.get("minimum_snapshot_generation"), int)
                or not isinstance(body.get("bot_token"), str)
                or not body.get("bot_token")
                or not isinstance(body.get("telegram_test_dc"), bool)
            ):
                self.respond(400, {"error": "invalid drain request"})
                return
            STATE["drain_requests"].append(
                {
                    "schema_version": body["schema_version"],
                    "token_lookup_hash": body["token_lookup_hash"],
                    "minimum_snapshot_generation": body[
                        "minimum_snapshot_generation"
                    ],
                    "telegram_test_dc": body["telegram_test_dc"],
                }
            )
            in_flight = int(STATE["gateway_in_flight"])
            official_fenced = bool(STATE["gateway_official_fenced"])
            official_requests = STATE["gateway_official_active_requests"]
            standard = official_requests.get("standard")
            local = official_requests.get("local")
            official_idle = standard == 0 and local == 0
            self.respond(
                200,
                {
                    "schema_version": 1,
                    "drained": in_flight == 0 and official_fenced and official_idle,
                    "snapshot_generation": "9223372036854775807",
                    "route_present": False,
                    "in_flight": str(in_flight),
                    "official_fenced": official_fenced,
                    "official_active_requests": {
                        "standard": None if standard is None else str(standard),
                        "local": None if local is None else str(local),
                    },
                },
            )
            return
        if path == "/__fail_next_close":
            STATE["fail_next_close"] = True
            self.respond(200, {"ok": True})
            return
        if path == "/__seed_webhook":
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b"{}"
            webhook = json.loads(body)
            test_dc = bool(webhook.pop("test_dc", False))
            bot_name = str(webhook.pop("bot", "manager"))
            STATE[self.webhook_key(bot_name, test_dc)] = webhook
            self.respond(200, {"ok": True})
            return
        if path == "/__downstream":
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b"{}"
            STATE["deliveries"].append(json.loads(body))
            self.respond(200, {"ok": True})
            return
        self.telegram_request()

    def telegram_request(self) -> None:
        parsed = urlparse(self.path)
        match = BOT_PATH.match(parsed.path)
        if not match:
            self.respond(404, {"ok": False, "error_code": 404, "description": "Not Found"})
            return
        token, test_marker, method = match.groups()
        test_dc = bool(test_marker)
        bot_name = self.bot_name(token, test_dc)
        if bot_name is None:
            self.respond(401, {"ok": False, "error_code": 401, "description": "Unauthorized"})
            return
        params = self.params(parsed.query)
        normalized = method.lower()
        STATE["calls"].append(
            {
                "bot": bot_name,
                "method": method,
                "params": params,
                "test_dc": test_dc,
                "credential": self.credential_label(token),
            }
        )
        if normalized == "getme":
            result = BOT_IDENTITIES[bot_name]
        elif normalized == "getmanagedbottoken":
            if bot_name != "manager" or int(params.get("user_id", 0)) != CHILD_BOT_ID:
                self.respond(
                    400,
                    {
                        "ok": False,
                        "error_code": 400,
                        "description": "Bad Request: managed bot not found",
                    },
                )
                return
            # This credential is deliberately returned only on the API wire. It
            # is never copied into STATE, request history, or HTTP logs.
            if test_dc:
                result = TEST_CHILD_TOKEN
            else:
                result = CHILD_ROTATED_TOKEN if STATE["rotated_child_token"] is True else CHILD_TOKEN
        elif normalized == "getwebhookinfo":
            webhook = self.webhook(bot_name, test_dc)
            result = {
                "url": webhook.get("url", ""),
                "has_custom_certificate": webhook.get("has_custom_certificate", False),
                "pending_update_count": 0,
            }
            for field in ("allowed_updates", "max_connections", "ip_address"):
                if field in webhook:
                    result[field] = webhook[field]
        elif normalized == "setwebhook":
            STATE[self.webhook_key(bot_name, test_dc)] = params
            result = True
        elif normalized == "deletewebhook":
            STATE[self.webhook_key(bot_name, test_dc)] = {}
            result = True
        elif normalized == "logout" and STATE["fail_next_logout"] is True:
            STATE["fail_next_logout"] = False
            self.respond(
                500,
                {
                    "ok": False,
                    "error_code": 500,
                    "description": "Internal Server Error: simulated ambiguous logout",
                },
            )
            return
        elif normalized == "close" and STATE["fail_next_close"] is True:
            STATE["fail_next_close"] = False
            self.respond(
                500,
                {
                    "ok": False,
                    "error_code": 500,
                    "description": "Internal Server Error: simulated ambiguous close",
                },
            )
            return
        elif normalized == "sendmessage":
            result = self.message_result(params, text=str(params.get("text", "")))
        elif normalized in {
            "sendphoto",
            "sendaudio",
            "senddocument",
            "sendvideo",
            "sendanimation",
            "sendvoice",
            "sendvideonote",
            "sendsticker",
        }:
            media_key = normalized.removeprefix("send")
            telegram_key = {"videonote": "video_note"}.get(media_key, media_key)
            media = {
                "file_id": f"{telegram_key}-file-1",
                "file_unique_id": f"{telegram_key}-unique-1",
                "file_size": 21,
            }
            if telegram_key in {"photo", "video", "animation", "video_note", "sticker"}:
                media.update({"width": 640, "height": 480})
            if telegram_key in {"video", "animation", "video_note", "audio", "voice"}:
                media["duration"] = 2
            result = self.message_result(
                params,
                caption=str(params.get("caption", "")) or None,
                **({"photo": [media]} if telegram_key == "photo" else {telegram_key: media}),
            )
        elif normalized == "sendmediagroup":
            media = params.get("media", [])
            if isinstance(media, str):
                media = json.loads(media)
            result = []
            for index, item in enumerate(media if isinstance(media, list) else []):
                media_type = str(item.get("type", "document"))
                descriptor = {
                    "file_id": f"{media_type}-album-{index + 1}",
                    "file_unique_id": f"{media_type}-album-unique-{index + 1}",
                    "file_size": 21,
                }
                if media_type in {"photo", "video"}:
                    descriptor.update({"width": 640, "height": 480})
                result.append(
                    self.message_result(
                        params,
                        caption=item.get("caption"),
                        media_group_id="album-e2e",
                        **({"photo": [descriptor]} if media_type == "photo" else {media_type: descriptor}),
                    )
                )
        elif normalized == "editmessagetext":
            result = self.message_result(
                params,
                message_id=int(params.get("message_id", 0)),
                text=str(params.get("text", "")),
                edit_date=1_786_620_100,
            )
        elif normalized == "editmessagecaption":
            result = self.message_result(
                params,
                message_id=int(params.get("message_id", 0)),
                caption=str(params.get("caption", "")),
                edit_date=1_786_620_100,
            )
        elif normalized == "sendpoll":
            options = params.get("options", [])
            if isinstance(options, str):
                options = json.loads(options)
            result = self.message_result(
                params,
                poll={
                    "id": "poll-e2e",
                    "question": params.get("question", "Question"),
                    "options": [
                        {"text": option.get("text", ""), "voter_count": 0}
                        for option in options
                    ],
                    "total_voter_count": 0,
                    "is_closed": False,
                    "is_anonymous": bool(params.get("is_anonymous", True)),
                    "type": "regular",
                    "allows_multiple_answers": bool(params.get("allows_multiple_answers", False)),
                },
            )
        elif normalized == "sendlocation":
            result = self.message_result(
                params,
                location={
                    "latitude": float(params.get("latitude", 0)),
                    "longitude": float(params.get("longitude", 0)),
                },
            )
        elif normalized == "sendcontact":
            result = self.message_result(
                params,
                contact={
                    "phone_number": params.get("phone_number", ""),
                    "first_name": params.get("first_name", ""),
                },
            )
        elif normalized == "senddice":
            result = self.message_result(
                params,
                dice={"emoji": params.get("emoji", "🎲"), "value": 6},
            )
        elif normalized in {
            "deleteMessage".lower(),
            "setMessageReaction".lower(),
            "sendChatAction".lower(),
        }:
            result = True
        elif normalized == "getfile":
            if params.get("file_id") == "native-local-file":
                native_directory = f"{token}:T" if test_dc else token
                file_path = f"/var/lib/telegram-bot-api/{native_directory}/documents/test.txt"
            else:
                file_path = os.environ.get("MOCK_LOCAL_FILE_PATH", "documents/test.txt")
            result = {"file_id": params.get("file_id", "file-1"), "file_unique_id": "unique-1", "file_size": 21, "file_path": file_path}
        else:
            result = {"method": method, "echo": params}
        self.respond(200, {"ok": True, "result": result})

    @staticmethod
    def bot_name(token: str, test_dc: bool) -> str | None:
        if token == (TEST_MANAGER_TOKEN if test_dc else MANAGER_TOKEN):
            return "manager"
        expected_child_tokens = (TEST_CHILD_TOKEN,) if test_dc else (CHILD_TOKEN, CHILD_ROTATED_TOKEN)
        if token in expected_child_tokens:
            return "child"
        return None

    @staticmethod
    def credential_label(token: str) -> str:
        if token == CHILD_ROTATED_TOKEN:
            return "rotated"
        return "current"

    @staticmethod
    def webhook_key(bot_name: str, test_dc: bool) -> str:
        prefix = "test_" if test_dc else ""
        return f"{prefix}{'webhook' if bot_name == 'manager' else 'child_webhook'}"

    def webhook(self, bot_name: str, test_dc: bool) -> dict[str, object]:
        webhook = STATE[self.webhook_key(bot_name, test_dc)]
        assert isinstance(webhook, dict)
        return webhook

    def params(self, query: str) -> dict[str, object]:
        values: dict[str, object] = {key: item[-1] for key, item in parse_qs(query).items()}
        body = self.read_body()
        content_type = self.headers.get("Content-Type", "")
        if body and content_type.startswith("application/json"):
            values.update(json.loads(body))
        elif body and content_type.startswith("application/x-www-form-urlencoded"):
            values.update({key: item[-1] for key, item in parse_qs(body.decode()).items()})
        elif body and content_type.startswith("multipart/form-data"):
            message = BytesParser(policy=email_policy).parsebytes(
                b"Content-Type: "
                + content_type.encode()
                + b"\r\nMIME-Version: 1.0\r\n\r\n"
                + body
            )
            for part in message.iter_parts():
                name = part.get_param("name", header="content-disposition")
                if not name:
                    continue
                payload = part.get_payload(decode=True) or b""
                filename = part.get_filename()
                if filename:
                    values[name] = {
                        "filename": filename,
                        "content_type": part.get_content_type(),
                        "size": len(payload),
                    }
                else:
                    values[name] = payload.decode("utf-8")
        return values

    def read_body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
            length = int(self.headers.get("Content-Length", "0"))
            return self.rfile.read(length) if length else b""
        chunks: list[bytes] = []
        while True:
            line = self.rfile.readline().strip().split(b";", 1)[0]
            if not line:
                continue
            size = int(line, 16)
            if size == 0:
                while self.rfile.readline() not in {b"\r\n", b"\n", b""}:
                    pass
                return b"".join(chunks)
            chunks.append(self.rfile.read(size))
            self.rfile.read(2)

    def message_result(
        self,
        params: dict[str, object],
        *,
        message_id: int | None = None,
        **fields: object,
    ) -> dict[str, object]:
        if message_id is None:
            message_id = int(STATE["next_message_id"])
            STATE["next_message_id"] = message_id + 1
        result: dict[str, object] = {
            "message_id": message_id,
            "date": 1_786_620_000,
            "chat": {"id": int(params.get("chat_id", 0)), "type": "private"},
        }
        for name in (
            "business_connection_id",
            "message_thread_id",
            "direct_messages_topic_id",
        ):
            if name in params:
                result[name] = params[name]
        result.update({key: value for key, value in fields.items() if value is not None})
        return result

    def respond(self, status: int, payload: object) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: object) -> None:
        return


if __name__ == "__main__":
    STATE["webhook"] = {}
    STATE["test_webhook"] = {}
    STATE["child_webhook"] = {}
    STATE["test_child_webhook"] = {}
    STATE["calls"] = []
    STATE["deliveries"] = []
    STATE["fail_next_logout"] = False
    STATE["fail_next_close"] = False
    STATE["rotated_child_token"] = False
    STATE["gateway_in_flight"] = 0
    STATE["gateway_official_fenced"] = True
    STATE["gateway_official_active_requests"] = {"standard": 0, "local": 0}
    STATE["drain_requests"] = []
    STATE["next_message_id"] = 9001
    port = int(os.environ.get("MOCK_TELEGRAM_PORT", "18081"))
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
