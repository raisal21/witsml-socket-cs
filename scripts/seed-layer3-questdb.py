#!/usr/bin/env python3
"""Seed QuestDB with deterministic 7-day history for frontend Layer 3 smoke.

This fixture is intentionally backend-owned because it mirrors the
`drill_samples` and `geo_samples` schema used by Persistence.cs and Tiles.cs.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import socket
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

DEFAULT_HTTP = "http://localhost:9000"
DEFAULT_ILP = "localhost:9009"
DEFAULT_SEED = 20260524
CADENCE_SECONDS = 30
DAYS = 7
FUTURE_BUFFER_MINUTES = 30
PAST_BUFFER_MINUTES = 5


@dataclass(frozen=True)
class Block:
    state: str
    start: datetime
    end: datetime
    rop_mhr: float
    rpm: float
    wob: float
    torque: float
    hkld: float
    spp: float
    gamma_bias: float


@dataclass(frozen=True)
class Row:
    ts: datetime
    seq: int
    state: str
    depth: float
    rpm: float
    wob: float
    torque: float
    hkld: float
    spp: float
    gamma: float
    rop: float
    h2s: float
    inc: float
    azi: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset and seed QuestDB sample tables for Layer 3 tile smoke."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Required. Drops/recreates drill_samples and geo_samples before seeding.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--http", default=DEFAULT_HTTP, help="QuestDB HTTP endpoint")
    parser.add_argument("--ilp", default=DEFAULT_ILP, help="QuestDB ILP host:port")
    parser.add_argument("--days", type=int, default=DAYS)
    parser.add_argument("--cadence-sec", type=int, default=CADENCE_SECONDS)
    parser.add_argument("--future-min", type=int, default=FUTURE_BUFFER_MINUTES)
    return parser.parse_args()


def exec_sql(http: str, sql: str, *, ignore_missing: bool = False) -> dict:
    url = f"{http.rstrip('/')}/exec?query={urllib.parse.quote(sql)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if ignore_missing and (
            "does not exist" in body.lower()
            or "table" in body.lower() and "not" in body.lower() and "exist" in body.lower()
        ):
            return {"ignored": body}
        raise RuntimeError(f"QuestDB SQL failed: {sql}\nHTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cannot reach QuestDB HTTP endpoint {http}: {exc}") from exc


def reset_schema(http: str) -> None:
    for table in ("drill_samples", "geo_samples"):
        exec_sql(http, f"DROP TABLE {table}", ignore_missing=True)

    exec_sql(
        http,
        """
        CREATE TABLE drill_samples (
          ts TIMESTAMP, seq LONG,
          depth FLOAT, rpm FLOAT, wob FLOAT, torque FLOAT, hkld FLOAT, spp FLOAT
        ) TIMESTAMP(ts) PARTITION BY DAY WAL
        """,
    )
    exec_sql(
        http,
        """
        CREATE TABLE geo_samples (
          ts TIMESTAMP, seq LONG,
          depth FLOAT, gamma FLOAT, rop FLOAT, h2s FLOAT, inc FLOAT, azi FLOAT
        ) TIMESTAMP(ts) PARTITION BY DAY WAL
        """,
    )


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def build_blocks(start: datetime, end: datetime, rng: random.Random) -> list[Block]:
    blocks: list[Block] = []
    t = start
    drilling_count = 0

    while t < end:
        drilling_count += 1
        drill_minutes = rng.uniform(90, 240)
        drill_end = min(end, t + timedelta(minutes=drill_minutes))
        blocks.append(
            Block(
                state="drilling",
                start=t,
                end=drill_end,
                rop_mhr=rng.uniform(3.0, 6.5),
                rpm=rng.uniform(85, 135),
                wob=rng.uniform(12, 24),
                torque=rng.uniform(3.5, 7.5),
                hkld=rng.uniform(185, 220),
                spp=rng.uniform(135, 205),
                gamma_bias=rng.uniform(-8, 8),
            )
        )
        t = drill_end
        if t >= end:
            break

        if drilling_count % 4 == 0:
            state = "standby"
            minutes = rng.uniform(120, 240)
        else:
            pick = rng.random()
            if pick < 0.45:
                state = "connection"
                minutes = rng.uniform(15, 45)
            elif pick < 0.75:
                state = "circulating"
                minutes = rng.uniform(30, 90)
            else:
                state = "standby"
                minutes = rng.uniform(60, 180)

        non_end = min(end, t + timedelta(minutes=minutes))
        if state == "connection":
            rpm, wob, torque, hkld, spp = rng.uniform(0, 8), rng.uniform(0, 2), rng.uniform(0, 0.8), rng.uniform(210, 240), rng.uniform(20, 60)
        elif state == "circulating":
            rpm, wob, torque, hkld, spp = rng.uniform(0, 20), rng.uniform(0, 4), rng.uniform(0.5, 2), rng.uniform(200, 225), rng.uniform(80, 160)
        else:
            rpm, wob, torque, hkld, spp = 0, 0, rng.uniform(0, 0.4), rng.uniform(205, 235), rng.uniform(0, 18)

        blocks.append(
            Block(
                state=state,
                start=t,
                end=non_end,
                rop_mhr=0,
                rpm=rpm,
                wob=wob,
                torque=torque,
                hkld=hkld,
                spp=spp,
                gamma_bias=rng.uniform(-10, 10),
            )
        )
        t = non_end

    return blocks


def block_for(blocks: list[Block], ts: datetime, idx: int) -> tuple[Block, int]:
    while idx + 1 < len(blocks) and ts >= blocks[idx].end:
        idx += 1
    return blocks[idx], idx


def lithology_gamma(depth: float, block_bias: float, rng: random.Random) -> float:
    band = int((depth - 1450) // 45)
    band_bias = ((band * 17) % 29) - 14
    slow = 6 * math.sin(depth / 85.0) + 3 * math.sin(depth / 27.0)
    return clamp(52 + band_bias + block_bias + slow + rng.uniform(-2.5, 2.5), 20, 150)


def generate_rows(start: datetime, end: datetime, cadence_sec: int, seed: int) -> tuple[list[Row], list[Block]]:
    rng = random.Random(seed)
    blocks = build_blocks(start, end, rng)

    raw_gain = sum(
        max(0.0, (b.end - b.start).total_seconds() / 3600.0) * b.rop_mhr
        for b in blocks
        if b.state == "drilling"
    )
    target_gain = rng.uniform(330, 380)
    rop_scale = target_gain / raw_gain if raw_gain > 0 else 1.0

    rows: list[Row] = []
    depth = 1500.0
    inc = 0.6
    azi = 45.0
    t = start
    seq = 0
    block_idx = 0

    while t <= end:
        b, block_idx = block_for(blocks, t, block_idx)
        state = b.state
        rop = b.rop_mhr * rop_scale if state == "drilling" else 0.0
        if rows:
            depth += rop * (cadence_sec / 3600.0)

        if state == "drilling":
            rpm = clamp(b.rpm + rng.uniform(-4, 4), 0, 180)
            wob = clamp(b.wob + rng.uniform(-1.5, 1.5), 0, 35)
            torque = clamp(b.torque + 0.05 * (wob - b.wob) + rng.uniform(-0.4, 0.4), 0, 12)
            spp = clamp(b.spp + rng.uniform(-6, 6), 0, 240)
            hkld = clamp(b.hkld + rng.uniform(-4, 4), 150, 260)
            rop_trace = clamp(rop + rng.uniform(-0.8, 0.8), 0.2, 12)
        elif state == "connection":
            rpm = clamp(b.rpm + rng.uniform(-1, 1), 0, 12)
            wob = clamp(b.wob + rng.uniform(-0.3, 0.3), 0, 4)
            torque = clamp(b.torque + rng.uniform(-0.1, 0.1), 0, 1.2)
            spp = clamp(b.spp + rng.uniform(-4, 4), 0, 70)
            hkld = clamp(b.hkld + rng.uniform(-3, 3), 180, 260)
            rop_trace = 0
        elif state == "circulating":
            rpm = clamp(b.rpm + rng.uniform(-2, 2), 0, 25)
            wob = clamp(b.wob + rng.uniform(-0.5, 0.5), 0, 5)
            torque = clamp(b.torque + rng.uniform(-0.2, 0.2), 0, 2.5)
            spp = clamp(b.spp + rng.uniform(-5, 5), 50, 180)
            hkld = clamp(b.hkld + rng.uniform(-3, 3), 180, 250)
            rop_trace = 0
        else:
            rpm = 0
            wob = 0
            torque = clamp(b.torque + rng.uniform(-0.05, 0.05), 0, 0.6)
            spp = clamp(b.spp + rng.uniform(-2, 2), 0, 25)
            hkld = clamp(b.hkld + rng.uniform(-2, 2), 180, 250)
            rop_trace = 0

        gamma = lithology_gamma(depth, b.gamma_bias, rng)
        h2s = clamp(rng.uniform(0.2, 4.5), 0, 60)
        if state == "drilling" and seq % 1739 in (0, 1, 2):
            h2s = rng.uniform(18, 42)

        inc = clamp(0.6 + (depth - 1500) / 110 + rng.uniform(-0.04, 0.04), 0, 8)
        azi = clamp(45 + (depth - 1500) / 30 + rng.uniform(-0.4, 0.4), 0, 360)

        rows.append(
            Row(
                ts=t,
                seq=seq,
                state=state,
                depth=depth,
                rpm=rpm,
                wob=wob,
                torque=torque,
                hkld=hkld,
                spp=spp,
                gamma=gamma,
                rop=rop_trace,
                h2s=h2s,
                inc=inc,
                azi=azi,
            )
        )
        seq += 1
        t += timedelta(seconds=cadence_sec)

    return rows, blocks


def ns(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000_000)


def f(v: float) -> str:
    return f"{v:.6f}".rstrip("0").rstrip(".")


def ilp_lines(rows: Iterable[Row]) -> Iterable[str]:
    for r in rows:
        tns = ns(r.ts)
        yield (
            f"drill_samples seq={r.seq}i"
            f",depth={f(r.depth)},rpm={f(r.rpm)},wob={f(r.wob)},torque={f(r.torque)}"
            f",hkld={f(r.hkld)},spp={f(r.spp)} {tns}\n"
        )
        yield (
            f"geo_samples seq={r.seq}i"
            f",depth={f(r.depth)},gamma={f(r.gamma)},rop={f(r.rop)},h2s={f(r.h2s)}"
            f",inc={f(r.inc)},azi={f(r.azi)} {tns}\n"
        )


def write_ilp(endpoint: str, rows: list[Row]) -> None:
    host, _, port_s = endpoint.partition(":")
    port = int(port_s or "9009")
    batch: list[str] = []
    with socket.create_connection((host, port), timeout=30) as sock:
        for line in ilp_lines(rows):
            batch.append(line)
            if len(batch) >= 5_000:
                sock.sendall("".join(batch).encode("utf-8"))
                batch.clear()
        if batch:
            sock.sendall("".join(batch).encode("utf-8"))


def first_value(result: dict):
    return result.get("dataset", [[None]])[0][0]


def verify(http: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for table in ("drill_samples", "geo_samples"):
        out[table] = exec_sql(http, f"select min(ts), max(ts), count() from {table}")
    return out


def dataset_row(result: dict) -> list:
    rows = result.get("dataset", [])
    return rows[0] if rows else []


def main() -> int:
    args = parse_args()
    if not args.reset:
        print("Refusing to reset QuestDB without explicit --reset.", file=sys.stderr)
        print("Run: scripts/seed-layer3-questdb.py --reset", file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(days=args.days, minutes=PAST_BUFFER_MINUTES)
    end = now + timedelta(minutes=args.future_min)

    print("[seed] Resetting drill_samples and geo_samples...")
    reset_schema(args.http)

    print(
        f"[seed] Generating deterministic fixture seed={args.seed} "
        f"range={start.isoformat()} → {end.isoformat()} cadence={args.cadence_sec}s"
    )
    rows, blocks = generate_rows(start, end, args.cadence_sec, args.seed)
    states = {s: sum(1 for b in blocks if b.state == s) for s in sorted({b.state for b in blocks})}

    print(f"[seed] Writing {len(rows)} drill rows + {len(rows)} geo rows via ILP {args.ilp}...")
    write_ilp(args.ilp, rows)

    # WAL visibility is usually fast, but give QuestDB a moment before summary queries.
    time.sleep(2)
    summary = verify(args.http)
    drill = dataset_row(summary["drill_samples"])
    geo = dataset_row(summary["geo_samples"])

    print("\nSeeded Layer 3 QuestDB fixture")
    print(f"  range requested : {start.isoformat()} → {end.isoformat()}")
    print(f"  cadence         : {args.cadence_sec}s")
    print(f"  seed            : {args.seed}")
    print(f"  states          : {states}")
    if drill:
        print(f"  drill_samples   : {drill[2]} rows · {drill[0]} → {drill[1]}")
    if geo:
        print(f"  geo_samples     : {geo[2]} rows · {geo[0]} → {geo[1]}")
    print("\nNext: run realtime-monitoring/scripts/smoke-test.sh and smoke 6h/12h/24h/7d presets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
