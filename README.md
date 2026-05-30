# witsml-socket-cs

C# port of the `realtime-monitoring/stubs/witsml-socket.ts` WebSocket telemetry
server. Same `ws://localhost:8080`, same JSON envelopes, same binary frame
layout — reimplemented in .NET 9 + ASP.NET Core. Adds QuestDB time-series
persistence, a `GET /api/tiles` aggregation endpoint for debug/manual queries,
and live historical tile snapshots/updates over the existing WebSocket.

**Requires .NET 9 SDK** (`9.0.1xx` or `9.0.3xx`). Check with `dotnet --list-sdks`.

## Quick start

```bash
# 1. Start QuestDB
~/questdb/bin/questdb.sh start -d ~/.questdb-root

# 2. Run the server
dotnet run
```

WS on `ws://0.0.0.0:8080`. QuestDB web console on `http://localhost:9000`.

## Commands

| Command | What |
|---|---|
| `dotnet build` | Build the project |
| `dotnet run` | Run the WebSocket telemetry server |

## Stack

| Layer | Technology |
|---|---|
| Runtime | .NET 9, ASP.NET Core |
| Transport | Kestrel + `System.Net.WebSockets` |
| Database | QuestDB (ILP TCP for writes, Pg-wire via Npgsql for queries) |
| Serialization | `System.Text.Json` |

## Auth

| Config | Method | Default |
|---|---|---|
| `Auth:HandshakeToken` | `appsettings.json`, `appsettings.Development.json`, or env `Auth__HandshakeToken` | `dev-token` (dev only) |

Clients must send a matching `token` in the HANDSHAKE payload. Fail-closed — an
unset or empty token rejects every handshake (`Program.cs` logs a startup
warning). Mismatch → `CLOSING` code `UNAUTHORIZED` (close 4401), non-retryable.

## What's in scope

- WS handshake, sub/unsub, binary broadcast (DRILL 101 / GEO 102), alarm
  raise/ack/purge, slow-client detection, graceful shutdown.
- QuestDB persistence via ILP TCP (writes) + HTTP `/exec` (schema bootstrap).
  Bounded `Channel<SampleRow>` with `DropOldest` so the broadcast hot path
  never blocks on DB lag. Auto-reconnect on socket failure.
- `GET /api/tiles` endpoint — pre-aggregated `min/max/avg` bins per trace
  via QuestDB `SAMPLE BY` over Pg-wire (Npgsql). Validates span × res
  before query. Kept unchanged for debug/manual fallback.
- WebSocket historical-live tile subscriptions. `TILE_SUBSCRIBE` returns an
  initial binary `201` snapshot per stream, then cadence-based binary `202`
  updates for the previous + current bucket. Raw `101`/`102` telemetry remains
  unchanged and can stay subscribed while tile mode is active.
- WebSocket history extent metadata. `HISTORY_EXTENT_REQUEST` returns actual
  QuestDB time/depth bounds for drill/geo so clients can clamp date pickers
  without static demo bounds.
- `RetentionJob` BackgroundService — drops QuestDB partitions older than
  `TimeSeries:RetentionDays` (default 31) on startup + every 24h. Safety
  guard rejects `< 1` (prevents live-data wipe).
- Static token auth validated in the HANDSHAKE payload.
- **Still out:** Timescale alt backend, TLS, real WITSML 2.x / ETP.

## Layout

```
Program.cs      host wiring + Kestrel + WS middleware + DI graph
Protocol.cs     constants, enums, envelopes, state machine
Domain.cs       RigState, AlarmRegistry, ConnectedClient
Hub.cs          WebSocketHub + FrameWriter (big-endian binary)
Handlers.cs     HANDSHAKE / SUBSCRIBE / UNSUBSCRIBE / TILE_* / ALARM_ACK
Services.cs     TelemetryService (10Hz) + PingService + AlarmPurgeService
Persistence.cs  SampleRow + ITimeSeriesStore + QuestDbStore + PersistenceService
Tiles.cs        TileResponse DTOs + ResolutionPicker + TileQueryService + /api/tiles
                RetentionJob lives in Persistence.cs (drops partitions older than N days)
```

## Smoke test

For frontend Layer 3 tile smoke, seed deterministic 30-day QuestDB history before
starting the dashboard:

```bash
scripts/seed-layer3-questdb.py --reset
```

The script resets only `drill_samples` and `geo_samples`, then writes 30-second
cadence fixture data via ILP TCP. It is intended for local smoke verification of
6h / 12h / 24h / 7d quick presets plus manual history ranges up to 30d.

Run the DB-free realism guard when changing the fixture generator:

```bash
scripts/seed-layer3-questdb.py --check-realism
```

Run the live historical tile WebSocket smoke with the server and QuestDB running:

```bash
scripts/smoke-tile-ws.py
```

Expected: `WELCOME`, raw `SUBSCRIBE_ACK`, `HISTORY_EXTENT`,
`TILE_SUBSCRIBE_ACK`, raw `101/102` frames, `201` tile snapshots for drill and
geo, then a `202` update whose `toUnixMs` advances. The script also checks that
`GET /api/tiles` still returns the original JSON shape and that a 30d
`TILE_RANGE_REQUEST` returns `6h` binary snapshots.

```js
import WebSocket from "ws";
const ws = new WebSocket("ws://localhost:8080/");
ws.on("open", () => {
  ws.send(JSON.stringify({ messageType: "HANDSHAKE", payload: { schemaId: 1, protocolVersion: 1, token: "dev-token" } }));
  setTimeout(() => ws.send(JSON.stringify({ messageType: "SUBSCRIBE", payload: { streams: [101, 102] } })), 50);
});
ws.on("message", (data, isBinary) => {
  if (isBinary) console.log("BIN", data[0], data.length);
  else console.log("JSON", JSON.parse(data.toString()).messageType);
});
```

Expected: `WELCOME` → `SUBSCRIBE_ACK` → binary frames at 10 Hz (drill 44B) /
1 Hz (geo 40B).

## Resilience

- **Ping/pong:** .NET WS lacks a ping/pong hook. Substitute = app-layer
  `HEARTBEAT` envelope every 10s + Kestrel `KeepAliveInterval=10s`. Any
  inbound msg resets `IsAlive` and demotes IDLE→ACTIVE.
- **Slow client:** per-client bounded `Channel<OutboundFrame>` (cap 64,
  `DropWrite`). `>5s` sustained backpressure → close `4429 SLOW_CLIENT`.
- **State machine:** every transition under `client.StateLock` since
  dispatcher / telemetry tick / ping run concurrently.
- **Custom close codes:** `(WebSocketCloseStatus)4400/4409/4429` casts — not
  standard but matches the TypeScript wire protocol.
- **Persistence backpressure:** `Channel<SampleRow>` cap 5000, `DropOldest`
  — if QuestDB lags or dies, telemetry keeps broadcasting and oldest
  unflushed samples are dropped. Drop counter logged every 60s.
- **DB outage:** on broken pipe, ILP TCP socket is disposed and reconnected
  on the next batch. Schema bootstrap retries up to 30× at 2s intervals
  before giving up.
- **ILP write semantics:** fire-and-forget per QuestDB ILP TCP — failures
  observed only via the TCP layer (broken pipe). Acceptable for telemetry;
  not for write-integrity-critical data.

## Tile API

```
GET /api/tiles?stream=<drill|geo>&from=<iso8601>&to=<iso8601>&res=<1s|10s|1m|5m|1h|6h>
```

Response:

```json
{
  "stream": "drill", "res": "1s",
  "from": "...", "to": "...",
  "bins": [
    {
      "ts": "2026-05-15T20:32:13.000Z",
      "depth": { "min": 1502.43, "max": 1502.52, "avg": 1502.48 },
      "rpm":   { "min": 115.30,  "max": 123.53,  "avg": 119.92 },
      "wob":   { "min": 18.10,   "max": 21.32,   "avg": 19.55  },
      "torque":{ "min": 4.08,    "max": 5.94,    "avg": 5.25   },
      "hkld":  { "min": 190.91,  "max": 206.07,  "avg": 199.88 },
      "spp":   { "min": 2470.70, "max": 2542.70, "avg": 2498.69 },
      "flow":  { "min": 1478.20, "max": 1522.40, "avg": 1504.35 }
    }
  ]
}
```

Drill traces: `depth, rpm, wob, torque, hkld, spp, flow`.
Geo traces: `depth, gamma, rop, h2s, inc, azi`.

### Resolution × max-span table

| res  | max span | max bins |
|------|----------|----------|
| 1s   | 2 min    | ~120     |
| 10s  | 20 min   | ~120     |
| 1m   | 2 h      | ~120     |
| 5m   | 12 h     | ~144     |
| 1h   | 7 d      | ~168     |
| 6h   | 30 d     | ~120     |

Out-of-range → 400 `RES_TOO_FINE` with the `accepted` list. Unknown stream →
400 `INVALID_STREAM`. Unparseable timestamp → 400 `INVALID_FROM` / `INVALID_TO`.

`min`/`max` are non-negotiable — they preserve safety-critical outliers
(HIGH_H2S spikes, SPP excursions) that pure averaging would smooth out.

## WebSocket Tile API

Control messages stay JSON. The frontend owns a non-zero `subscriptionId`.

```json
{ "messageType": "TILE_SUBSCRIBE", "payload": { "subscriptionId": 1, "spanMinutes": 360, "res": "5m", "streams": ["drill", "geo"] } }
{ "messageType": "TILE_UNSUBSCRIBE", "payload": { "subscriptionId": 1 } }
{ "messageType": "TILE_RANGE_REQUEST", "payload": { "subscriptionId": 101, "fromUnixMs": 1768000000000, "toUnixMs": 1770592000000, "res": "6h", "streams": ["drill", "geo"] } }
```

Valid live tile presets:

| spanMinutes | res | cadence |
|-------------|-----|---------|
| 360 / 720 | 5m | 5000 ms |
| 1440 / 4320 / 10080 | 1h | 30000 ms |

Accepted subscribe reply:

```json
{ "messageType": "TILE_SUBSCRIBE_ACK", "payload": { "subscriptionId": 1, "accepted": ["drill", "geo"], "rejected": [], "spanMinutes": 360, "res": "5m", "cadenceMs": 5000 } }
```

`TILE_RANGE_REQUEST` is a manual-history snapshot path for date-picker and
well-profile exploration beyond the quick preset horizon. It validates the same
resolution max-span ladder, returns binary `201` snapshot frames using the
request `subscriptionId`, and does not register a live subscription or emit
`202` updates.

Tile binary frames are big-endian and use a fixed 40-byte header:

| Offset | Type | Meaning |
|--------|------|---------|
| 0 | u8 | frameType: `201` snapshot, `202` update |
| 1 | u8 | version = `1` |
| 2 | u16 | flags = `0` |
| 4 | u32 | subscriptionId |
| 8 | u8 | stream: `1` drill, `2` geo |
| 9 | u8 | resCode: `1s=1`, `10s=2`, `1m=3`, `5m=4`, `1h=5`, `6h=6` |
| 10 | u16 | traceMask: drill `0x007f`, geo `0x003f` |
| 12 | i64 | fromUnixMs |
| 20 | i64 | toUnixMs |
| 28 | i64 | replaceFromUnixMs |
| 36 | u32 | binCount |

Each bin is `i64 tsUnixMs`, then `f32 min`, `f32 max`, `f32 avg` for each
enabled trace in fixed order. Drill order is `depth, rpm, wob, torque, hkld,
spp, flow`; geo order is `depth, gamma, rop, h2s, inc, azi`. Null stats are
encoded as `NaN`.

## WebSocket History Extent API

History extent is low-volume metadata, so it uses the existing JSON control
channel:

```json
{ "messageType": "HISTORY_EXTENT_REQUEST", "payload": { "wellId": "ga-01", "streams": ["drill", "geo"] } }
```

Response:

```json
{
  "messageType": "HISTORY_EXTENT",
  "timestamp": 1770600000000,
  "payload": {
    "wellId": "ga-01",
    "streams": {
      "drill": { "minTimeMs": 1770000000000, "maxTimeMs": 1770600000000, "minDepth": 1292.1, "maxDepth": 1411.5 },
      "geo": { "minTimeMs": 1770000010000, "maxTimeMs": 1770599990000, "minDepth": 1292.4, "maxDepth": 1411.2 }
    },
    "shared": {
      "minTimeMs": 1770000010000,
      "maxTimeMs": 1770599990000,
      "minDepth": 1292.1,
      "maxDepth": 1411.5,
      "timeMode": "intersection",
      "depthSource": "drill"
    },
    "warnings": []
  }
}
```

Per-stream extents query `min(ts)`, `max(ts)`, `min(depth)`, and `max(depth)`
from `drill_samples` / `geo_samples`. `shared` time uses overlap intersection
when possible, a single stream when only one has data, and union with warning
`STREAM_TIME_RANGES_DO_NOT_OVERLAP` when streams do not overlap. `shared` depth
uses drill when present and falls back to geo. Empty tables return `null`
extent fields, not static placeholder dates.

## Verified behavior

```
QuestDB up      → 10 Hz drill + 1 Hz geo land in drill_samples / geo_samples
QuestDB down    → broadcast continues at full rate, droppedFrames=0
QuestDB back    → auto-reconnect, "write recovered after N failures" log line
Tile happy path → 1066 raw rows → 25 1s-bins via SAMPLE BY in <50 ms
Tile bad input  → 400 with code + message; never returns 500 for known errors
Retention=5     → 10-day-old partition dropped, today's partition kept
Retention=0     → disabled with WARN log, no DDL run
```

## Related

- [realtime-monitoring](https://github.com/raisal21/realtime-monitoring) —
  React dashboard frontend (the primary consumer of this server)
