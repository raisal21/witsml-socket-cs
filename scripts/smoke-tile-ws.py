#!/usr/bin/env python3
import base64
import json
import os
import random
import socket
import struct
import sys
import time
import urllib.parse
import urllib.request


WS_URL = os.environ.get("WS_URL", "ws://localhost:8080/")
WS_TOKEN = os.environ.get("WS_TOKEN", os.environ.get("Auth__HandshakeToken", "dev-token"))
SUBSCRIPTION_ID = int(os.environ.get("TILE_SUBSCRIPTION_ID", "1"))
RANGE_SUBSCRIPTION_ID = int(os.environ.get("TILE_RANGE_SUBSCRIPTION_ID", "101"))
DRILL_TILE_MASK = 0x007F
GEO_TILE_MASK = 0x003F
SIX_HOUR_RES_CODE = 6


def main() -> int:
    ws = WebSocket(WS_URL)
    ws.connect()
    try:
        ws.send_json({
            "messageType": "HANDSHAKE",
            "payload": {
                "schemaId": 1,
                "protocolVersion": 1,
                "token": WS_TOKEN,
            },
        })
        expect_json(ws, "WELCOME", 5)

        ws.send_json({"messageType": "SUBSCRIBE", "payload": {"streams": [101, 102]}})
        expect_json(ws, "SUBSCRIBE_ACK", 5)

        ws.send_json({
            "messageType": "HISTORY_EXTENT_REQUEST",
            "payload": {"wellId": "ga-01", "streams": ["drill", "geo"]},
        })
        assert_history_extent(expect_json(ws, "HISTORY_EXTENT", 5))

        ws.send_json({
            "messageType": "TILE_SUBSCRIBE",
            "payload": {
                "subscriptionId": SUBSCRIPTION_ID,
                "spanMinutes": 360,
                "res": "5m",
                "streams": ["drill", "geo"],
            },
        })

        deadline = time.time() + 20
        tile_ack = None
        raw_seen = set()
        snapshots = {}
        update = None
        errors = []

        while time.time() < deadline and (tile_ack is None or len(snapshots) < 2 or update is None or raw_seen != {101, 102}):
            opcode, payload = ws.recv_frame(deadline - time.time())
            if opcode == 1:
                msg = json.loads(payload.decode("utf-8"))
                if msg.get("messageType") == "TILE_SUBSCRIBE_ACK":
                    tile_ack = msg["payload"]
                elif msg.get("messageType") == "ERROR":
                    errors.append(msg.get("error", {}))
                continue
            if opcode != 2:
                continue

            frame_type = payload[0]
            if frame_type in (101, 102):
                raw_seen.add(frame_type)
            elif frame_type in (201, 202):
                header = parse_tile_header(payload)
                if header["subscriptionId"] != SUBSCRIPTION_ID:
                    raise AssertionError(f"unexpected tile subscription id: {header}")
                if frame_type == 201:
                    snapshots[header["stream"]] = header
                else:
                    update = header

        if tile_ack is None:
            raise AssertionError(f"missing TILE_SUBSCRIBE_ACK errors={errors}")
        if sorted(tile_ack["accepted"]) != ["drill", "geo"]:
            raise AssertionError(f"tile streams not accepted: {tile_ack} errors={errors}")
        if raw_seen != {101, 102}:
            raise AssertionError(f"missing raw frames while tile active: {raw_seen}")
        if set(snapshots) != {1, 2}:
            raise AssertionError(f"missing tile snapshots: {snapshots}")
        if snapshots[1]["traceMask"] != DRILL_TILE_MASK:
            raise AssertionError(f"unexpected drill tile mask: {snapshots[1]}")
        if snapshots[2]["traceMask"] != GEO_TILE_MASK:
            raise AssertionError(f"unexpected geo tile mask: {snapshots[2]}")
        if update is None:
            raise AssertionError("missing TILE_UPDATE")
        first_to = min(header["toUnixMs"] for header in snapshots.values())
        if update["toUnixMs"] <= first_to:
            raise AssertionError(f"update toUnixMs did not advance: snapshot={first_to} update={update}")

        assert_rest_shape()
        assert_custom_range_request(ws)
        print("OK tile ws smoke: history extent, live tiles, custom 30d range, raw frames, masks, REST flow shape")
        return 0
    finally:
        ws.close()


def expect_json(ws, message_type: str, timeout: float) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        opcode, payload = ws.recv_frame(deadline - time.time())
        if opcode != 1:
            continue
        msg = json.loads(payload.decode("utf-8"))
        if msg.get("messageType") == message_type:
            return msg
        if msg.get("messageType") == "ERROR":
            raise AssertionError(f"server error before {message_type}: {msg}")
        if msg.get("messageType") == "CLOSING":
            raise AssertionError(f"server closing before {message_type}: {msg}")
    raise TimeoutError(f"timed out waiting for {message_type}")


def parse_tile_header(payload: bytes) -> dict:
    if len(payload) < 40:
        raise AssertionError(f"short tile frame: {len(payload)}")
    values = struct.unpack(">BBHIBBHqqqI", payload[:40])
    return {
        "frameType": values[0],
        "version": values[1],
        "flags": values[2],
        "subscriptionId": values[3],
        "stream": values[4],
        "resCode": values[5],
        "traceMask": values[6],
        "fromUnixMs": values[7],
        "toUnixMs": values[8],
        "replaceFromUnixMs": values[9],
        "binCount": values[10],
    }


def assert_history_extent(msg: dict) -> None:
    payload = msg.get("payload", {})
    if payload.get("wellId") != "ga-01":
        raise AssertionError(f"unexpected history wellId: {payload}")
    streams = payload.get("streams", {})
    if not {"drill", "geo"}.issubset(streams):
        raise AssertionError(f"history extent missing stream keys: {payload}")
    shared = payload.get("shared", {})
    for key in ("minTimeMs", "maxTimeMs", "minDepth", "maxDepth"):
        if key not in shared:
            raise AssertionError(f"history shared extent missing {key}: {payload}")
    if "timeMode" not in shared or "depthSource" not in shared:
        raise AssertionError(f"history shared metadata missing: {payload}")


def assert_rest_shape() -> None:
    now_ms = int(time.time() * 1000)
    from_iso = iso(now_ms - 360 * 60_000)
    to_iso = iso(now_ms)
    params = urllib.parse.urlencode({
        "stream": "drill",
        "from": from_iso,
        "to": to_iso,
        "res": "5m",
    })
    with urllib.request.urlopen(f"http://localhost:8080/api/tiles?{params}", timeout=10) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    for key in ("stream", "res", "from", "to", "bins"):
        if key not in body:
            raise AssertionError(f"REST tile response missing {key}: {body}")
    if not body["bins"]:
        raise AssertionError(f"REST drill tile response has no bins: {body}")
    first = body["bins"][0]
    if "ts" not in first or "depth" not in first or "flow" not in first:
        raise AssertionError(f"REST drill tile bin shape changed: {first}")


def assert_custom_range_request(ws) -> None:
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - 30 * 24 * 60 * 60_000
    ws.send_json({
        "messageType": "TILE_RANGE_REQUEST",
        "payload": {
            "subscriptionId": RANGE_SUBSCRIPTION_ID,
            "fromUnixMs": from_ms,
            "toUnixMs": now_ms,
            "res": "6h",
            "streams": ["drill", "geo"],
        },
    })

    deadline = time.time() + 20
    snapshots = {}
    errors = []
    while time.time() < deadline and len(snapshots) < 2:
        opcode, payload = ws.recv_frame(deadline - time.time())
        if opcode == 1:
            msg = json.loads(payload.decode("utf-8"))
            if msg.get("messageType") == "ERROR":
                error = msg.get("error", {})
                if error.get("code") in {
                    "INVALID_PAYLOAD",
                    "INVALID_TILE_RANGE",
                    "INVALID_TILE_STREAM",
                    "TILE_RANGE_FAILED",
                }:
                    errors.append(error)
            continue
        if opcode != 2 or payload[0] != 201:
            continue

        header = parse_tile_header(payload)
        if header["subscriptionId"] != RANGE_SUBSCRIPTION_ID:
            continue
        snapshots[header["stream"]] = header

    if errors:
        raise AssertionError(f"custom tile range errors: {errors}")
    if set(snapshots) != {1, 2}:
        raise AssertionError(f"missing custom range snapshots: {snapshots}")
    for stream, header in snapshots.items():
        expected_mask = DRILL_TILE_MASK if stream == 1 else GEO_TILE_MASK
        if header["resCode"] != SIX_HOUR_RES_CODE:
            raise AssertionError(f"custom range did not use 6h res: {header}")
        if header["traceMask"] != expected_mask:
            raise AssertionError(f"custom range mask changed: {header}")
        if header["fromUnixMs"] != from_ms or header["toUnixMs"] != now_ms:
            raise AssertionError(f"custom range header did not preserve requested range: {header}")
        if header["binCount"] <= 0:
            raise AssertionError(f"custom range returned no bins: {header}")


def iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(ms / 1000))


class WebSocket:
    def __init__(self, url: str):
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != "ws":
            raise ValueError("Only ws:// URLs are supported by this smoke script")
        self.host = parsed.hostname or "localhost"
        self.port = parsed.port or 80
        self.path = parsed.path or "/"
        if parsed.query:
            self.path += "?" + parsed.query
        self.sock: socket.socket | None = None

    def connect(self) -> None:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        sock = socket.create_connection((self.host, self.port), timeout=5)
        request = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        sock.sendall(request.encode("ascii"))
        response = sock.recv(4096)
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            raise ConnectionError(response.decode("latin1", errors="replace"))
        self.sock = sock

    def send_json(self, obj: dict) -> None:
        self.send_frame(1, json.dumps(obj, separators=(",", ":")).encode("utf-8"))

    def send_frame(self, opcode: int, payload: bytes) -> None:
        if self.sock is None:
            raise RuntimeError("socket is not connected")
        first = 0x80 | opcode
        mask_key = random.randbytes(4) if hasattr(random, "randbytes") else os.urandom(4)
        length = len(payload)
        if length < 126:
            header = struct.pack(">BB", first, 0x80 | length)
        elif length <= 0xffff:
            header = struct.pack(">BBH", first, 0x80 | 126, length)
        else:
            header = struct.pack(">BBQ", first, 0x80 | 127, length)
        masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask_key + masked)

    def recv_frame(self, timeout: float) -> tuple[int, bytes]:
        if self.sock is None:
            raise RuntimeError("socket is not connected")
        if timeout <= 0:
            raise TimeoutError("timed out waiting for frame")
        self.sock.settimeout(timeout)
        first, second = self._read_exact(2)
        opcode = first & 0x0f
        masked = (second & 0x80) != 0
        length = second & 0x7f
        if length == 126:
            length = struct.unpack(">H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if masked else b""
        payload = self._read_exact(length)
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        if opcode == 9:
            self.send_frame(10, payload)
            return self.recv_frame(timeout)
        if opcode == 8:
            raise ConnectionError("server closed websocket")
        return opcode, payload

    def _read_exact(self, length: int) -> bytes:
        if self.sock is None:
            raise RuntimeError("socket is not connected")
        chunks = []
        remaining = length
        while remaining:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise ConnectionError("socket closed")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def close(self) -> None:
        if self.sock is None:
            return
        try:
            self.send_frame(8, b"")
            self.sock.close()
        finally:
            self.sock = None


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FAIL tile ws smoke: {exc}", file=sys.stderr)
        raise SystemExit(1)
