# witsml-socket-cs

C# port of `realtime-monitoring/stubs/witsml-socket.ts` — Phase 1 (wire-protocol parity).

Replaces the Node + `ws` TypeScript stub with .NET 9 + ASP.NET Core. Same `ws://localhost:8080`, same JSON envelopes, same binary frame layout. Existing UI clients work unchanged.

## Scope

- **In:** WS handshake, subscribe/unsubscribe, binary broadcast (DRILL=101 / GEO=102), alarm raise/ack/purge, slow-client detection, graceful shutdown.
- **Out (deferred to Phase 2/3 per `CSHARP_PORT_PLAN.md`):** QuestDB persistence, `/api/tiles` HTTP, retention job.

## Run

```bash
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="$HOME/.dotnet"
dotnet run
```

Server listens on `ws://0.0.0.0:8080`.

## Layout

```
Program.cs    host wiring + Kestrel + WS middleware
Protocol.cs   constants, enums, envelopes, state machine
Domain.cs     RigState, AlarmRegistry, ConnectedClient
Hub.cs        WebSocketHub + FrameWriter (big-endian binary)
Handlers.cs   HANDSHAKE / SUBSCRIBE / UNSUBSCRIBE / ALARM_ACK
Services.cs   TelemetryService (10Hz) + PingService + AlarmPurgeService
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
