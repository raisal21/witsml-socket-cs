using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace WitsmlSocket;

internal sealed class WebSocketHub
{
    private readonly ILogger<WebSocketHub> _log;
    private readonly AlarmRegistry _alarms;
    private readonly RigState _state;

    public ConcurrentDictionary<string, ConnectedClient> Clients { get; } = new();
    public ConcurrentDictionary<StreamDef, ConcurrentDictionary<string, ConnectedClient>> StreamSubs { get; } = new();
    public TileQueryService Tiles { get; }
    public HistoryExtentService HistoryExtents { get; }

    public string HandshakeToken { get; }

    public WebSocketHub(
        ILogger<WebSocketHub> log,
        AlarmRegistry alarms,
        RigState state,
        IConfiguration cfg,
        TileQueryService tiles,
        HistoryExtentService historyExtents)
    {
        _log = log;
        _alarms = alarms;
        _state = state;
        Tiles = tiles;
        HistoryExtents = historyExtents;
        HandshakeToken = cfg["Auth:HandshakeToken"] ?? "";
        foreach (StreamDef s in Enum.GetValues<StreamDef>())
            StreamSubs[s] = new();
    }

    public async Task HandleAsync(WebSocket ws, string remote, CancellationToken hostCt)
    {
        var clientId = $"rig-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds():x}-{Random.Shared.Next(0xFFFF):x4}";
        var channel = Channel.CreateBounded<OutboundFrame>(new BoundedChannelOptions(Constants.OutboundQueueDepth)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
        });

        var client = new ConnectedClient
        {
            ClientId = clientId,
            Socket = ws,
            Outbound = channel,
            State = ClientState.Connecting,
            LastActivityUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        Clients[clientId] = client;
        _log.LogInformation("[CONNECT] {Id} from {Remote}", clientId, remote);

        StateMachine.TryTransition(ref client.State, ClientState.Handshaking);

        client.HandshakeTimer = CancellationTokenSource.CreateLinkedTokenSource(hostCt);
        _ = HandshakeTimeoutAsync(client);

        using var connCt = CancellationTokenSource.CreateLinkedTokenSource(hostCt);
        var sender = Task.Run(() => SendLoopAsync(client, connCt.Token));

        try
        {
            await ReceiveLoopAsync(client, connCt.Token);
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException) { }
        catch (Exception ex)
        {
            _log.LogError(ex, "[WS ERROR] {Id}", clientId);
        }
        finally
        {
            await connCt.CancelAsync();
            channel.Writer.TryComplete();
            try { await sender; } catch { }

            foreach (var s in client.Subscriptions)
                StreamSubs[s].TryRemove(clientId, out _);
            client.TileSubscriptions.Clear();
            Clients.TryRemove(clientId, out _);
            client.HandshakeTimer?.Dispose();
            client.SendGate.Dispose();

            lock (client.StateLock)
                StateMachine.TryTransition(ref client.State, ClientState.Closed);

            _log.LogInformation("[DISCONNECT] {Id} droppedFrames={Drop}", clientId, client.DroppedFrames);
        }
    }

    private async Task HandshakeTimeoutAsync(ConnectedClient client)
    {
        try
        {
            await Task.Delay(Constants.HandshakeTimeoutMs, client.HandshakeTimer!.Token);
            if (client.State == ClientState.Handshaking)
            {
                _log.LogWarning("[HANDSHAKE] Timeout {Id}", client.ClientId);
                await CloseAsync(client, "HANDSHAKE_TIMEOUT", 1008, "Handshake timeout", retryable: true);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task ReceiveLoopAsync(ConnectedClient client, CancellationToken ct)
    {
        var buf = new byte[8192];
        using var ms = new MemoryStream();

        while (client.Socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            ms.SetLength(0);
            WebSocketReceiveResult res;
            do
            {
                res = await client.Socket.ReceiveAsync(buf, ct);
                if (res.MessageType == WebSocketMessageType.Close)
                {
                    await client.Socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
                    return;
                }
                ms.Write(buf, 0, res.Count);
            } while (!res.EndOfMessage);

            client.LastActivityUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            if (res.MessageType != WebSocketMessageType.Text)
            {
                await SendErrorAsync(client, "INVALID_MESSAGE", "Binary inbound not supported");
                continue;
            }

            ClientMessage? msg;
            try
            {
                msg = JsonSerializer.Deserialize<ClientMessage>(ms.ToArray(), JsonOpts.Default);
            }
            catch
            {
                await CloseAsync(client, "INVALID_JSON", 4400, "Message is not valid JSON", false);
                return;
            }

            if (msg is null || string.IsNullOrEmpty(msg.messageType))
            {
                await SendErrorAsync(client, "INVALID_MESSAGE", "messageType is required");
                continue;
            }

            if (client.State == ClientState.Handshaking && msg.messageType != "HANDSHAKE")
            {
                _log.LogWarning("[PROTOCOL] {Id} sent {Type} before HANDSHAKE", client.ClientId, msg.messageType);
                await CloseAsync(client, "HANDSHAKE_REQUIRED", 4400, "Expected HANDSHAKE as first message", false);
                return;
            }

            await Handlers.DispatchAsync(this, client, msg, _alarms);

            if (client.State == ClientState.Idle)
            {
                lock (client.StateLock)
                    StateMachine.TryTransition(ref client.State, ClientState.Active);
            }
        }
    }

    private static async Task SendLoopAsync(ConnectedClient client, CancellationToken ct)
    {
        try
        {
            await foreach (var frame in client.Outbound.Reader.ReadAllAsync(ct))
            {
                if (client.Socket.State != WebSocketState.Open) return;
                var type = frame.IsBinary ? WebSocketMessageType.Binary : WebSocketMessageType.Text;
                await client.SendGate.WaitAsync(ct);
                try
                {
                    if (client.Socket.State != WebSocketState.Open) return;
                    await client.Socket.SendAsync(frame.Payload, type, endOfMessage: true, ct);
                }
                finally { client.SendGate.Release(); }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    // =============================================================================
    // Send helpers
    // =============================================================================
    public ValueTask SendJsonAsync<T>(ConnectedClient client, string messageType, T? payload, ErrorPayload? error = null)
    {
        var envelope = new ServerMessage<T>
        {
            messageType = messageType,
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            payload = payload,
            error = error,
        };
        var bytes = JsonSerializer.SerializeToUtf8Bytes(envelope, JsonOpts.Default);
        return EnqueueAsync(client, new OutboundFrame(false, bytes));
    }

    public ValueTask SendErrorAsync(ConnectedClient client, string code, string message)
        => SendJsonAsync<object>(client, "ERROR", null, new ErrorPayload { code = code, message = message });

    public ValueTask SendBinaryAsync(ConnectedClient client, ReadOnlyMemory<byte> payload)
        => EnqueueAsync(client, new OutboundFrame(true, payload));

    public async Task CloseAsync(ConnectedClient client, string code, int closeCode, string reason, bool retryable)
    {
        var envelope = new ServerMessage<ClosingPayload>
        {
            messageType = "CLOSING",
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            payload = new ClosingPayload
            {
                code = code,
                reason = reason,
                retryable = retryable,
                closeCode = closeCode,
            },
        };
        var bytes = JsonSerializer.SerializeToUtf8Bytes(envelope, JsonOpts.Default);

        lock (client.StateLock)
            StateMachine.TryTransition(ref client.State, ClientState.Closing);

        // Send CLOSING then close under SendGate — the gate is shared with the send
        // loop, so the CLOSING frame is flushed before the close handshake (no race).
        await client.SendGate.WaitAsync();
        try
        {
            if (client.Socket.State == WebSocketState.Open)
            {
                await client.Socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
                await client.Socket.CloseAsync((WebSocketCloseStatus)closeCode, reason, CancellationToken.None);
            }
        }
        catch { }
        finally { client.SendGate.Release(); }

        client.HandshakeTimer?.Cancel();
    }

    private async ValueTask EnqueueAsync(ConnectedClient client, OutboundFrame frame)
    {
        if (!client.Outbound.Writer.TryWrite(frame))
        {
            client.DroppedFrames++;
            if (client.SlowSinceUtc is null)
                client.SlowSinceUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var slowFor = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - client.SlowSinceUtc.Value;
            if (slowFor > Constants.SlowTimeoutMs)
            {
                await CloseAsync(client, "SLOW_CLIENT", 4429, "Client too slow — backoff and retry", true);
            }
        }
        else
        {
            client.SlowSinceUtc = null;
        }
    }

    // =============================================================================
    // Broadcast
    // =============================================================================
    public void BroadcastBinary(StreamDef stream, ReadOnlySpan<byte> frame)
    {
        var subs = StreamSubs[stream];
        if (subs.IsEmpty) return;

        var copy = frame.ToArray();
        var ro = new ReadOnlyMemory<byte>(copy);

        foreach (var (_, client) in subs)
        {
            if (client.Socket.State != WebSocketState.Open) continue;
            if (client.State != ClientState.Active) continue;
            _ = EnqueueAsync(client, new OutboundFrame(true, ro)).AsTask();
        }
    }

    public void BroadcastJson<T>(string messageType, T payload)
    {
        foreach (var (_, client) in Clients)
        {
            if (client.Socket.State != WebSocketState.Open) continue;
            if (client.State != ClientState.Active && client.State != ClientState.Idle) continue;
            _ = SendJsonAsync(client, messageType, payload).AsTask();
        }
    }

    public async Task ShutdownAllAsync()
    {
        foreach (var (_, c) in Clients)
        {
            await CloseAsync(c, "SERVER_SHUTDOWN", 1001, "Server shutting down", true);
        }
    }
}

// =============================================================================
// Binary frame writer — big-endian, layout per README.md wire format
// =============================================================================
internal static class FrameWriter
{
    public static void WriteDrill(Span<byte> dst, RigState s, uint seq)
    {
        dst[0] = (byte)StreamDef.Drill;
        dst[1] = Constants.ProtocolVersion;
        dst[2] = 0; dst[3] = 0;
        BinaryPrimitives.WriteUInt32BigEndian(dst[4..],  seq);
        BinaryPrimitives.WriteUInt64BigEndian(dst[8..],  (ulong)s.TimestampUnixMs);
        BinaryPrimitives.WriteSingleBigEndian(dst[16..], s.Depth);
        BinaryPrimitives.WriteSingleBigEndian(dst[20..], s.Rpm);
        BinaryPrimitives.WriteSingleBigEndian(dst[24..], s.Wob);
        BinaryPrimitives.WriteSingleBigEndian(dst[28..], s.Torque);
        BinaryPrimitives.WriteSingleBigEndian(dst[32..], s.Hkld);
        BinaryPrimitives.WriteSingleBigEndian(dst[36..], s.Spp);
        BinaryPrimitives.WriteSingleBigEndian(dst[40..], s.Flow);
    }

    public static void WriteGeo(Span<byte> dst, RigState s, uint seq)
    {
        dst[0] = (byte)StreamDef.Geo;
        dst[1] = Constants.ProtocolVersion;
        dst[2] = 0; dst[3] = 0;
        BinaryPrimitives.WriteUInt32BigEndian(dst[4..],  seq);
        BinaryPrimitives.WriteUInt64BigEndian(dst[8..],  (ulong)s.TimestampUnixMs);
        BinaryPrimitives.WriteSingleBigEndian(dst[16..], s.Depth);
        BinaryPrimitives.WriteSingleBigEndian(dst[20..], s.Gamma);
        BinaryPrimitives.WriteSingleBigEndian(dst[24..], s.Rop);
        BinaryPrimitives.WriteSingleBigEndian(dst[28..], s.H2s);
        BinaryPrimitives.WriteSingleBigEndian(dst[32..], s.Inc);
        BinaryPrimitives.WriteSingleBigEndian(dst[36..], s.Azi);
    }

    public static byte[] WriteTile(
        TileFrameType frameType,
        TileSubscription sub,
        TileStream stream,
        TileResponse response,
        long fromUnixMs,
        long toUnixMs,
        long replaceFromUnixMs)
    {
        var traces = TileQueryService.TraceOrder(stream);
        var binSize = sizeof(long) + traces.Length * 3 * sizeof(float);
        var bytes = new byte[Constants.TileHeaderBytes + response.bins.Count * binSize];
        var dst = bytes.AsSpan();

        dst[0] = (byte)frameType;
        dst[1] = Constants.ProtocolVersion;
        dst[2] = 0; dst[3] = 0;
        BinaryPrimitives.WriteUInt32BigEndian(dst[4..], sub.SubscriptionId);
        dst[8] = (byte)stream;
        dst[9] = (byte)ResolutionPicker.ToCode(sub.Res);
        BinaryPrimitives.WriteUInt16BigEndian(dst[10..], Constants.TileTraceMask(stream));
        BinaryPrimitives.WriteInt64BigEndian(dst[12..], fromUnixMs);
        BinaryPrimitives.WriteInt64BigEndian(dst[20..], toUnixMs);
        BinaryPrimitives.WriteInt64BigEndian(dst[28..], replaceFromUnixMs);
        BinaryPrimitives.WriteUInt32BigEndian(dst[36..], (uint)response.bins.Count);

        var offset = Constants.TileHeaderBytes;
        foreach (var bin in response.bins)
        {
            var ts = DateTimeOffset.Parse(bin.ts, CultureInfo.InvariantCulture).ToUnixTimeMilliseconds();
            BinaryPrimitives.WriteInt64BigEndian(dst[offset..], ts);
            offset += sizeof(long);

            foreach (var trace in traces)
            {
                var stat = bin.Traces.TryGetValue(trace, out var value) ? value as TileStat : null;
                WriteStat(dst[offset..], stat?.min);
                WriteStat(dst[(offset + sizeof(float))..], stat?.max);
                WriteStat(dst[(offset + 2 * sizeof(float))..], stat?.avg);
                offset += 3 * sizeof(float);
            }
        }

        return bytes;
    }

    private static void WriteStat(Span<byte> dst, double? value)
        => BinaryPrimitives.WriteSingleBigEndian(dst, value is null ? float.NaN : (float)value.Value);
}
