# witsml-socket-cs

C# port of `realtime-monitoring/stubs/witsml-socket.ts` — Phases 1 + 2 + 3 (wire / persistence / tiles).

Replaces the Node + `ws` TypeScript stub with .NET 9 + ASP.NET Core. Same `ws://localhost:8080`, same JSON envelopes, same binary frame layout. Adds QuestDB time-series persistence on a side channel, plus `GET /api/tiles` aggregation endpoint for wide-zoom queries.

## Scope

- **Phase 1 (in):** WS handshake, sub/unsub, binary broadcast (DRILL=101 / GEO=102), alarm raise/ack/purge, slow-client detection, graceful shutdown.
- **Phase 2 (in):** QuestDB persistence via ILP TCP (writes) + HTTP `/exec` (schema bootstrap). Bounded `Channel<SampleRow>` with `DropOldest` so broadcast hot path never blocks on DB lag. Auto-reconnect on socket failure.
- **Phase 3 (in):** `GET /api/tiles` endpoint — pre-aggregated `min/max/avg` bins per trace via QuestDB `SAMPLE BY` over Pg-wire (Npgsql). Validates span × res before query.
- **Still out:** retention job, Timescale alt backend, auth, real WITSML 2.x / ETP.

## Run

```bash
# 1. Start QuestDB (one-time install via download already in ~/questdb)
~/questdb/bin/questdb.sh start -d ~/.questdb-root

# 2. Run the C# service
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="$HOME/.dotnet"
dotnet run
```

WS on `ws://0.0.0.0:8080`. QuestDB web console on `http://localhost:9000`.

## Layout

```
Program.cs      host wiring + Kestrel + WS middleware + DI graph
Protocol.cs     constants, enums, envelopes, state machine
Domain.cs       RigState, AlarmRegistry, ConnectedClient
Hub.cs          WebSocketHub + FrameWriter (big-endian binary)
Handlers.cs     HANDSHAKE / SUBSCRIBE / UNSUBSCRIBE / ALARM_ACK
Services.cs     TelemetryService (10Hz) + PingService + AlarmPurgeService
Persistence.cs  SampleRow + ITimeSeriesStore + QuestDbStore + PersistenceService
Tiles.cs        TileResponse DTOs + ResolutionPicker + TilesController (/api/tiles)
```

## Smoke test

```js
import WebSocket from "ws";
const ws = new WebSocket("ws://localhost:8080/");
ws.on("open", () => {
  ws.send(JSON.stringify({ messageType: "HANDSHAKE", payload: { schemaId: 1, protocolVersion: 1 } }));
  setTimeout(() => ws.send(JSON.stringify({ messageType: "SUBSCRIBE", payload: { streams: [101, 102] } })), 50);
});
ws.on("message", (data, isBinary) => {
  if (isBinary) console.log("BIN", data[0], data.length);
  else console.log("JSON", JSON.parse(data.toString()).messageType);
});
```

Expected: `WELCOME` → `SUBSCRIBE_ACK` → binary frames at 10 Hz (drill 44B) / 1 Hz (geo 40B).

## Edge-case notes (per plan §Edge Cases)

- **Ping/pong:** .NET WS lacks a ping/pong hook. Substitute = app-layer `HEARTBEAT` envelope every 10s + Kestrel `KeepAliveInterval=10s`. Any inbound msg resets `IsAlive` and demotes IDLE→ACTIVE.
- **Slow client:** per-client bounded `Channel<OutboundFrame>` (cap 64, `DropWrite`). `>5s` sustained backpressure → close `4429 SLOW_CLIENT`.
- **State machine:** every transition under `client.StateLock` since dispatcher / telemetry tick / ping run concurrently.
- **Custom close codes:** `(WebSocketCloseStatus)4400/4409/4429` casts — not standard but matches TS wire.
- **Persistence backpressure:** `Channel<SampleRow>` cap 5000, `DropOldest` — if QuestDB lags or dies, telemetry keeps broadcasting and oldest unflushed samples are dropped. Drop counter logged every 60s.
- **DB outage:** on broken pipe, ILP TCP socket is disposed and reconnected on the next batch. Schema bootstrap retries up to 30× at 2s intervals before giving up.
- **ILP write semantics:** fire-and-forget per QuestDB ILP TCP — failures observed only via the TCP layer (broken pipe). Acceptable for telemetry; not for write-integrity-critical data.

## Tile API

```
GET /api/tiles?stream=<drill|geo>&from=<iso8601>&to=<iso8601>&res=<1s|10s|1m|5m|1h>
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
      "spp":   { "min": 2470.70, "max": 2542.70, "avg": 2498.69 }
    }
  ]
}
```

Geo traces: `depth, gamma, rop, h2s, inc, azi`.

### Resolution × max-span table

| res  | max span | max bins |
|------|----------|----------|
| 1s   | 2 min    | ~120     |
| 10s  | 20 min   | ~120     |
| 1m   | 2 h      | ~120     |
| 5m   | 12 h     | ~144     |
| 1h   | 7 d      | ~168     |

Out-of-range → 400 `RES_TOO_FINE` with the `accepted` list. Unknown stream → 400 `INVALID_STREAM`. Unparseable timestamp → 400 `INVALID_FROM`/`INVALID_TO`.

`min`/`max` are non-negotiable per `CHART_INTERACTION_ANALYSIS.md` §11 Q10 — they preserve safety-critical outliers (HIGH_H2S spikes, SPP excursions) that pure averaging would smooth out.

## Verified behavior

```
QuestDB up      → 10 Hz drill + 1 Hz geo land in drill_samples / geo_samples
QuestDB down    → broadcast continues at full rate, droppedFrames=0
QuestDB back    → auto-reconnect, "write recovered after N failures" log line
Tile happy path → 1066 raw rows → 25 1s-bins via SAMPLE BY in <50 ms
Tile bad input  → 400 with code + message; never returns 500 for known errors
```
