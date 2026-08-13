#!/usr/bin/env python3
"""Tiny Telegram Bot API contract double for local end-to-end checks."""

from __future__ import annotations

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


STATE: dict[str, object] = {"webhook": {}, "calls": [], "deliveries": []}
BOT_PATH = re.compile(r"^/bot([^/]+)/([^/]+)$")
FILE_PATH = re.compile(r"^/file/bot([^/]+)/(.+)$")
EXPECTED_TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path == "/__state":
            self.respond(200, STATE)
            return
        file_match = FILE_PATH.match(urlparse(self.path).path)
        if file_match:
            if file_match.group(1) != EXPECTED_TOKEN:
                self.respond(401, {"ok": False, "error_code": 401, "description": "Unauthorized"})
                return
            data = b"phenogram-test-file\n"
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.telegram_request()

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/__reset":
            STATE["webhook"] = {}
            STATE["calls"] = []
            STATE["deliveries"] = []
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
        token, method = match.groups()
        if token != EXPECTED_TOKEN:
            self.respond(401, {"ok": False, "error_code": 401, "description": "Unauthorized"})
            return
        params = self.params(parsed.query)
        normalized = method.lower()
        STATE["calls"].append({"method": method, "params": params})
        if normalized == "getme":
            result = {"id": 123456789, "is_bot": True, "first_name": "Phenogram Test", "username": "phenogram_test_bot"}
        elif normalized == "getwebhookinfo":
            webhook = STATE["webhook"]
            result = {"url": webhook.get("url", ""), "has_custom_certificate": False, "pending_update_count": 0}
        elif normalized == "setwebhook":
            STATE["webhook"] = params
            result = True
        elif normalized == "deletewebhook":
            STATE["webhook"] = {}
            result = True
        elif normalized == "sendmessage":
            result = {
                "message_id": 9001,
                "date": 1_786_620_000,
                "chat": {"id": int(params.get("chat_id", 0)), "type": "private"},
                "text": params.get("text", ""),
            }
        elif normalized == "getfile":
            file_path = os.environ.get("MOCK_LOCAL_FILE_PATH", "documents/test.txt")
            result = {"file_id": params.get("file_id", "file-1"), "file_unique_id": "unique-1", "file_size": 21, "file_path": file_path}
        else:
            result = {"method": method, "echo": params}
        self.respond(200, {"ok": True, "result": result})

    def params(self, query: str) -> dict[str, object]:
        values: dict[str, object] = {key: item[-1] for key, item in parse_qs(query).items()}
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        content_type = self.headers.get("Content-Type", "")
        if body and content_type.startswith("application/json"):
            values.update(json.loads(body))
        elif body and content_type.startswith("application/x-www-form-urlencoded"):
            values.update({key: item[-1] for key, item in parse_qs(body.decode()).items()})
        return values

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
    STATE["calls"] = []
    STATE["deliveries"] = []
    port = int(os.environ.get("MOCK_TELEGRAM_PORT", "18081"))
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
