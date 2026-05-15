using System.Text.Json;

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
            "ALARM_ACK"   => HandleAlarmAckAsync(hub, client, msg.payload, alarms),
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
