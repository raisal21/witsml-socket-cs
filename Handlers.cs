using System.Text.Json;
using Npgsql;

namespace WitsmlSocket;

internal static class Handlers
{
    public static Task DispatchAsync(WebSocketHub hub, ConnectedClient client, ClientMessage msg, AlarmRegistry alarms)
    {
        return msg.messageType switch
        {
            "HANDSHAKE"   => HandleHandshakeAsync(hub, client, msg.payload),
            "SUBSCRIBE"   => HandleSubscribeAsync(hub, client, msg.payload),
            "UNSUBSCRIBE" => HandleUnsubscribeAsync(hub, client, msg.payload),
            "TILE_SUBSCRIBE" => HandleTileSubscribeAsync(hub, client, msg.payload),
            "TILE_UNSUBSCRIBE" => HandleTileUnsubscribeAsync(hub, client, msg.payload),
            "TILE_RANGE_REQUEST" => HandleTileRangeRequestAsync(hub, client, msg.payload),
            "HISTORY_EXTENT_REQUEST" => HandleHistoryExtentRequestAsync(hub, client, msg.payload),
            "ALARM_ACK"   => HandleAlarmAckAsync(hub, client, msg.payload, alarms),
            "HEARTBEAT"   => Task.CompletedTask,
            _             => HandleUnknownAsync(hub, client, msg.messageType ?? ""),
        };
    }

    private static async Task HandleHandshakeAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        if (client.State != ClientState.Handshaking) return;
        client.HandshakeTimer?.Cancel();

        HandshakePayload? hs;
        try { hs = payload.Deserialize<HandshakePayload>(JsonOpts.Default); }
        catch
        {
            await hub.CloseAsync(client, "INVALID_SCHEMA", 4400, "schemaId must be a number", false);
            return;
        }

        if (hs is null || hs.schemaId != Constants.SupportedSchemaId)
        {
            await hub.CloseAsync(client, "UNSUPPORTED_SCHEMA", 4409, "Unsupported schema version", false);
            return;
        }

        if (hs.protocolVersion != Constants.ProtocolVersion)
        {
            await hub.CloseAsync(client, "UNSUPPORTED_PROTOCOL", 4409, "Unsupported protocol version", false);
            return;
        }

        if (string.IsNullOrEmpty(hub.HandshakeToken) || hs.token != hub.HandshakeToken)
        {
            await hub.CloseAsync(client, "UNAUTHORIZED", Constants.UnauthorizedClose, "Invalid or missing token", false);
            return;
        }

        if (!string.IsNullOrEmpty(hs.clientId)) client.ClientId = hs.clientId;

        lock (client.StateLock)
            StateMachine.TryTransition(ref client.State, ClientState.Active);

        await hub.SendJsonAsync(client, "WELCOME", new WelcomePayload
        {
            status = "OK",
            clientId = client.ClientId,
            availableStreams = Enum.GetValues<StreamDef>().Select(s => (int)s).ToArray(),
            serverVersion = Constants.ProtocolVersion,
        });
    }

    private static async Task HandleSubscribeAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        if (client.State != ClientState.Active && client.State != ClientState.Idle)
        {
            await hub.SendErrorAsync(client, "INVALID_STATE", "Client must be ACTIVE or IDLE to subscribe");
            return;
        }

        SubscribePayload? sp;
        try { sp = payload.Deserialize<SubscribePayload>(JsonOpts.Default); } catch { sp = null; }
        if (sp is null)
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "streams must be an array");
            return;
        }

        var accepted = new List<int>();
        var rejected = new List<int>();
        foreach (var id in sp.streams)
        {
            if (Enum.IsDefined((StreamDef)id))
            {
                var s = (StreamDef)id;
                lock (client.StateLock) client.Subscriptions.Add(s);
                hub.StreamSubs[s][client.ClientId] = client;
                accepted.Add(id);
            }
            else
            {
                rejected.Add(id);
            }
        }

        await hub.SendJsonAsync(client, "SUBSCRIBE_ACK", new SubscribeAckPayload
        {
            accepted = [.. accepted],
            rejected = [.. rejected],
            currentSubscriptions = client.Subscriptions.Select(s => (int)s).ToArray(),
        });
    }

    private static async Task HandleUnsubscribeAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        if (client.State != ClientState.Active && client.State != ClientState.Idle)
        {
            await hub.SendErrorAsync(client, "INVALID_STATE", "Client must be ACTIVE or IDLE to unsubscribe");
            return;
        }

        SubscribePayload? sp;
        try { sp = payload.Deserialize<SubscribePayload>(JsonOpts.Default); } catch { sp = null; }
        if (sp is null)
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "streams must be an array");
            return;
        }

        var removed = new List<int>();
        var notFound = new List<int>();
        foreach (var id in sp.streams)
        {
            var s = (StreamDef)id;
            bool had;
            lock (client.StateLock) had = client.Subscriptions.Remove(s);
            if (had)
            {
                hub.StreamSubs[s].TryRemove(client.ClientId, out _);
                removed.Add(id);
            }
            else
            {
                notFound.Add(id);
            }
        }

        await hub.SendJsonAsync(client, "UNSUBSCRIBE_ACK", new UnsubscribeAckPayload
        {
            removed = [.. removed],
            notFound = [.. notFound],
            currentSubscriptions = client.Subscriptions.Select(s => (int)s).ToArray(),
        });
    }

    private static async Task HandleHistoryExtentRequestAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        if (client.State != ClientState.Active && client.State != ClientState.Idle)
        {
            await hub.SendErrorAsync(client, "INVALID_STATE", "Client must be ACTIVE or IDLE to request history extents");
            return;
        }

        HistoryExtentRequestPayload? request;
        try { request = payload.Deserialize<HistoryExtentRequestPayload>(JsonOpts.Default); } catch { request = null; }
        if (request is null)
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "wellId and streams are required");
            return;
        }

        var acceptedStreams = new List<TileStream>();
        var rejected = new List<string>();
        var requestedStreams = request.streams.Length == 0 ? ["drill", "geo"] : request.streams;
        foreach (var raw in requestedStreams.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (TileQueryService.TryParseStream(raw, out var stream))
                acceptedStreams.Add(stream);
            else
                rejected.Add(raw);
        }

        if (acceptedStreams.Count == 0)
        {
            await hub.SendErrorAsync(client, "INVALID_HISTORY_STREAM", "streams must include drill or geo");
            return;
        }
        if (rejected.Count > 0)
        {
            await hub.SendErrorAsync(client, "INVALID_HISTORY_STREAM", $"unsupported streams: {string.Join(", ", rejected)}");
            return;
        }

        try
        {
            var response = await hub.HistoryExtents.QueryAsync(request.wellId, [.. acceptedStreams], CancellationToken.None);
            await hub.SendJsonAsync(client, "HISTORY_EXTENT", response);
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            await hub.SendErrorAsync(client, "HISTORY_EXTENT_FAILED", "History extent query failed");
        }
    }

    private static async Task HandleTileSubscribeAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        if (client.State != ClientState.Active && client.State != ClientState.Idle)
        {
            await hub.SendErrorAsync(client, "INVALID_STATE", "Client must be ACTIVE or IDLE to subscribe to tiles");
            return;
        }

        TileSubscribePayload? tp;
        try { tp = payload.Deserialize<TileSubscribePayload>(JsonOpts.Default); } catch { tp = null; }
        if (tp is null || tp.subscriptionId == 0)
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "subscriptionId must be a non-zero uint");
            return;
        }

        if (client.TileSubscriptions.ContainsKey(tp.subscriptionId))
        {
            await hub.SendErrorAsync(client, "DUPLICATE_TILE_SUBSCRIPTION", $"Tile subscription {tp.subscriptionId} is already active");
            return;
        }

        if (!ResolutionPicker.TryGet(tp.res, out var bucket) ||
            !TileQueryService.IsValidLiveCombo(tp.spanMinutes, tp.res, out var cadenceMs))
        {
            await hub.SendErrorAsync(client, "INVALID_TILE_RANGE", "spanMinutes/res must be 6h or 12h at 5m, or 24h/3d/7d at 1h");
            return;
        }

        var acceptedStreams = new List<TileStream>();
        var rejected = new List<string>();
        foreach (var raw in tp.streams.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (TileQueryService.TryParseStream(raw, out var stream))
                acceptedStreams.Add(stream);
            else
                rejected.Add(raw);
        }

        if (acceptedStreams.Count == 0)
        {
            await hub.SendJsonAsync(client, "TILE_SUBSCRIBE_ACK", new TileSubscribeAckPayload
            {
                subscriptionId = tp.subscriptionId,
                accepted = [],
                rejected = [.. rejected],
                spanMinutes = tp.spanMinutes,
                res = tp.res,
                cadenceMs = cadenceMs,
            });
            await hub.SendErrorAsync(client, "INVALID_TILE_STREAM", "streams must include drill or geo");
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var from = now.AddMinutes(-tp.spanMinutes);
        var snapshots = new List<(TileStream Stream, TileResponse Response)>();
        var queryAccepted = new List<TileStream>();
        var snapshotFailures = new List<string>();
        foreach (var stream in acceptedStreams)
        {
            try
            {
                var response = await hub.Tiles.QueryAsync(stream, from, now, tp.res, CancellationToken.None);
                snapshots.Add((stream, response));
                queryAccepted.Add(stream);
            }
            catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
            {
                var streamName = TileQueryService.StreamName(stream);
                rejected.Add(streamName);
                snapshotFailures.Add(streamName);
            }
        }

        var sub = new TileSubscription
        {
            SubscriptionId = tp.subscriptionId,
            SpanMinutes = tp.spanMinutes,
            Res = tp.res,
            Bucket = bucket,
            CadenceMs = cadenceMs,
            Streams = [.. queryAccepted],
            NextDueUnixMs = now.ToUnixTimeMilliseconds() + cadenceMs,
        };

        if (queryAccepted.Count > 0 && !client.TileSubscriptions.TryAdd(tp.subscriptionId, sub))
        {
            await hub.SendErrorAsync(client, "DUPLICATE_TILE_SUBSCRIPTION", $"Tile subscription {tp.subscriptionId} is already active");
            return;
        }

        await hub.SendJsonAsync(client, "TILE_SUBSCRIBE_ACK", new TileSubscribeAckPayload
        {
            subscriptionId = tp.subscriptionId,
            accepted = queryAccepted.Select(TileQueryService.StreamName).ToArray(),
            rejected = [.. rejected],
            spanMinutes = tp.spanMinutes,
            res = tp.res,
            cadenceMs = cadenceMs,
        });

        if (queryAccepted.Count == 0)
        {
            await hub.SendErrorAsync(client, "TILE_SNAPSHOT_FAILED", "History store is unavailable");
            return;
        }
        if (snapshotFailures.Count > 0)
        {
            await hub.SendErrorAsync(client, "TILE_SNAPSHOT_FAILED", $"History snapshot failed for: {string.Join(", ", snapshotFailures)}");
        }

        var fromMs = from.ToUnixTimeMilliseconds();
        var toMs = now.ToUnixTimeMilliseconds();
        foreach (var (stream, response) in snapshots)
        {
            var bytes = FrameWriter.WriteTile(TileFrameType.Snapshot, sub, stream, response, fromMs, toMs, fromMs);
            await hub.SendBinaryAsync(client, bytes);
        }
    }

    private static async Task HandleTileRangeRequestAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        if (client.State != ClientState.Active && client.State != ClientState.Idle)
        {
            await hub.SendErrorAsync(client, "INVALID_STATE", "Client must be ACTIVE or IDLE to request tile ranges");
            return;
        }

        TileRangeRequestPayload? tp;
        try { tp = payload.Deserialize<TileRangeRequestPayload>(JsonOpts.Default); } catch { tp = null; }
        var subscriptionId = tp?.subscriptionId is > 0 ? tp.subscriptionId : (tp?.requestId ?? 0);
        if (tp is null || subscriptionId == 0)
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "subscriptionId or requestId must be a non-zero uint");
            return;
        }

        DateTimeOffset from;
        DateTimeOffset to;
        try
        {
            from = DateTimeOffset.FromUnixTimeMilliseconds(tp.fromUnixMs);
            to = DateTimeOffset.FromUnixTimeMilliseconds(tp.toUnixMs);
        }
        catch (ArgumentOutOfRangeException)
        {
            await hub.SendErrorAsync(client, "INVALID_TILE_RANGE", "fromUnixMs/toUnixMs must be valid epoch milliseconds");
            return;
        }

        if (to <= from)
        {
            await hub.SendErrorAsync(client, "INVALID_TILE_RANGE", "toUnixMs must be greater than fromUnixMs");
            return;
        }

        if (!ResolutionPicker.TryGet(tp.res, out var bucket) ||
            !ResolutionPicker.ValidateSpan(tp.res, to - from, out _))
        {
            await hub.SendErrorAsync(client, "INVALID_TILE_RANGE", "manual range is too wide for the requested resolution");
            return;
        }

        var acceptedStreams = new List<TileStream>();
        var rejected = new List<string>();
        foreach (var raw in tp.streams.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (TileQueryService.TryParseStream(raw, out var stream))
                acceptedStreams.Add(stream);
            else
                rejected.Add(raw);
        }

        if (acceptedStreams.Count == 0 || rejected.Count > 0)
        {
            await hub.SendErrorAsync(client, "INVALID_TILE_STREAM", "streams must include only drill and/or geo");
            return;
        }

        var sub = new TileSubscription
        {
            SubscriptionId = subscriptionId,
            SpanMinutes = (int)Math.Ceiling((to - from).TotalMinutes),
            Res = tp.res,
            Bucket = bucket,
            CadenceMs = 0,
            Streams = [.. acceptedStreams],
            NextDueUnixMs = 0,
        };

        try
        {
            foreach (var stream in acceptedStreams)
            {
                var response = await hub.Tiles.QueryAsync(stream, from, to, tp.res, CancellationToken.None);
                var bytes = FrameWriter.WriteTile(
                    TileFrameType.Snapshot,
                    sub,
                    stream,
                    response,
                    tp.fromUnixMs,
                    tp.toUnixMs,
                    tp.fromUnixMs);
                await hub.SendBinaryAsync(client, bytes);
            }
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            await hub.SendErrorAsync(client, "TILE_RANGE_FAILED", "History range query failed");
        }
    }

    private static async Task HandleTileUnsubscribeAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload)
    {
        TileUnsubscribePayload? tp;
        try { tp = payload.Deserialize<TileUnsubscribePayload>(JsonOpts.Default); } catch { tp = null; }
        if (tp is null || tp.subscriptionId == 0)
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "subscriptionId must be a non-zero uint");
            return;
        }

        var removed = client.TileSubscriptions.TryRemove(tp.subscriptionId, out _);
        await hub.SendJsonAsync(client, "TILE_UNSUBSCRIBE_ACK", new TileUnsubscribeAckPayload
        {
            subscriptionId = tp.subscriptionId,
            removed = removed,
        });
    }

    private static async Task HandleAlarmAckAsync(WebSocketHub hub, ConnectedClient client, JsonElement payload, AlarmRegistry alarms)
    {
        if (client.State != ClientState.Active && client.State != ClientState.Idle)
        {
            await hub.SendErrorAsync(client, "INVALID_STATE", "Client must be ACTIVE or IDLE to acknowledge alarms");
            return;
        }

        AlarmAckPayload? ack;
        try { ack = payload.Deserialize<AlarmAckPayload>(JsonOpts.Default); } catch { ack = null; }
        if (ack is null || string.IsNullOrEmpty(ack.alarmId) || string.IsNullOrEmpty(ack.operatorName) || string.IsNullOrEmpty(ack.role))
        {
            await hub.SendErrorAsync(client, "INVALID_PAYLOAD", "alarmId, operatorName, role must be valid strings");
            return;
        }

        if (!alarms.TryGet(ack.alarmId, out var existing) || existing is null)
        {
            await hub.SendErrorAsync(client, "ALARM_NOT_FOUND", $"Alarm {ack.alarmId} not found");
            return;
        }

        if (!alarms.Acknowledge(ack.alarmId, ack.operatorName, ack.role, out var updated) || updated is null)
        {
            await hub.SendErrorAsync(client, "ALREADY_ACKED", $"Alarm {ack.alarmId} already acknowledged");
            return;
        }

        hub.BroadcastJson("ALARM_ACKED", new AlarmEventPayload { alarm = updated.ToDto() });
    }

    private static Task HandleUnknownAsync(WebSocketHub hub, ConnectedClient client, string messageType)
        => hub.SendErrorAsync(client, "UNKNOWN_MESSAGE_TYPE", $"Unsupported messageType: {messageType}").AsTask();
}
