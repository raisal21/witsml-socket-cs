using System.Text.Json;
using System.Text.Json.Serialization;

namespace WitsmlSocket;

internal static class JsonOpts
{
    public static readonly JsonSerializerOptions Default = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = false,
    };
}

internal static class Constants
{
    public const int Port = 8080;
    public const byte ProtocolVersion = 1;
    public const int SupportedSchemaId = 1;
    public const int UnauthorizedClose = 4401;
    public const int HandshakeTimeoutMs = 5_000;
    public const int IdleTimeoutMs = 15_000;
    public const int PingIntervalMs = 10_000;
    public const int LivenessTimeoutMs = 30_000;
    public const int SlowTimeoutMs = 5_000;
    public const int OutboundQueueDepth = 64;
    public const int TelemetryTickMs = 100;
    public const int GeoTickRatio = 10;
    public const double AlarmRaiseProbability = 0.005;
    public static readonly TimeSpan AlarmRetention = TimeSpan.FromHours(24);
    public const int DrillFrameBytes = 44;
    public const int GeoFrameBytes = 40;
}

internal enum ClientState
{
    Connecting,
    Handshaking,
    Active,
    Idle,
    Closing,
    Closed,
}

internal enum StreamDef
{
    Drill = 101,
    Geo = 102,
}

internal enum AlarmSeverity
{
    INFO,
    WARNING,
    CRITICAL,
}

internal static class StateMachine
{
    private static readonly Dictionary<ClientState, ClientState[]> Valid = new()
    {
        [ClientState.Connecting]  = [ClientState.Handshaking, ClientState.Closing],
        [ClientState.Handshaking] = [ClientState.Active, ClientState.Closing, ClientState.Closed],
        [ClientState.Active]      = [ClientState.Idle, ClientState.Closing, ClientState.Closed],
        [ClientState.Idle]        = [ClientState.Active, ClientState.Closing, ClientState.Closed],
        [ClientState.Closing]     = [ClientState.Closed],
        [ClientState.Closed]      = [],
    };

    public static bool TryTransition(ref ClientState current, ClientState next)
    {
        if (current == ClientState.Closed) return false;
        if (!Valid[current].Contains(next)) return false;
        current = next;
        return true;
    }
}

// =============================================================================
// JSON envelopes — wire-compat with TS stub
// =============================================================================

internal sealed class ClientMessage
{
    public string? messageType { get; set; }
    public string? requestId { get; set; }
    public System.Text.Json.JsonElement payload { get; set; }
}

internal sealed class ServerMessage<T>
{
    public string messageType { get; set; } = "";
    public long timestamp { get; set; }
    public T? payload { get; set; }
    public ErrorPayload? error { get; set; }
}

internal sealed class ErrorPayload
{
    public string code { get; set; } = "";
    public string message { get; set; } = "";
}

internal sealed class HandshakePayload
{
    public int schemaId { get; set; }
    public int protocolVersion { get; set; }
    public string? clientId { get; set; }
    public string? token { get; set; }
}

internal sealed class WelcomePayload
{
    public string status { get; set; } = "OK";
    public string clientId { get; set; } = "";
    public int[] availableStreams { get; set; } = [];
    public int serverVersion { get; set; }
}

internal sealed class SubscribePayload
{
    public int[] streams { get; set; } = [];
}

internal sealed class SubscribeAckPayload
{
    public int[] accepted { get; set; } = [];
    public int[] rejected { get; set; } = [];
    public int[] currentSubscriptions { get; set; } = [];
}

internal sealed class UnsubscribeAckPayload
{
    public int[] removed { get; set; } = [];
    public int[] notFound { get; set; } = [];
    public int[] currentSubscriptions { get; set; } = [];
}

internal sealed class AlarmAckPayload
{
    public string alarmId { get; set; } = "";
    public string operatorName { get; set; } = "";
    public string role { get; set; } = "";
    public long timestamp { get; set; }
}

internal sealed class AlarmAcknowledgement
{
    public string operatorName { get; set; } = "";
    public string role { get; set; } = "";
    public long timestamp { get; set; }
}

internal sealed class AlarmDto
{
    public string id { get; set; } = "";
    public string code { get; set; } = "";
    public string message { get; set; } = "";
    public string severity { get; set; } = "";
    public long raisedAt { get; set; }
    public bool acknowledged { get; set; }
    public AlarmAcknowledgement? acknowledgedBy { get; set; }
}

internal sealed class AlarmEventPayload
{
    public AlarmDto alarm { get; set; } = new();
}

internal sealed class ClosingPayload
{
    public string code { get; set; } = "";
    public string reason { get; set; } = "";
    public bool retryable { get; set; }
    public int? retryAfterMs { get; set; }
    public int closeCode { get; set; }
}

