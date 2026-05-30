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
    public const int TileHeaderBytes = 40;
    public const ushort DrillTileTraceMask = 0x007f;
    public const ushort GeoTileTraceMask = 0x003f;
    public const int TileShortCadenceMs = 5_000;
    public const int TileLongCadenceMs = 30_000;
    public const int TileUpdateTickMs = 1_000;
    public const int TileUpdateErrorThrottleMs = 60_000;

    public static ushort TileTraceMask(TileStream stream) => stream switch
    {
        TileStream.Drill => DrillTileTraceMask,
        TileStream.Geo => GeoTileTraceMask,
        _ => throw new ArgumentOutOfRangeException(nameof(stream), stream, "unknown tile stream"),
    };
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

internal enum TileFrameType : byte
{
    Snapshot = 201,
    Update = 202,
}

internal enum TileStream : byte
{
    Drill = 1,
    Geo = 2,
}

internal enum TileResCode : byte
{
    OneSecond = 1,
    TenSeconds = 2,
    OneMinute = 3,
    FiveMinutes = 4,
    OneHour = 5,
    SixHours = 6,
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

internal sealed class TileSubscribePayload
{
    public uint subscriptionId { get; set; }
    public int spanMinutes { get; set; }
    public string res { get; set; } = "";
    public string[] streams { get; set; } = [];
}

internal sealed class TileUnsubscribePayload
{
    public uint subscriptionId { get; set; }
}

internal sealed class TileRangeRequestPayload
{
    public uint requestId { get; set; }
    public uint subscriptionId { get; set; }
    public long fromUnixMs { get; set; }
    public long toUnixMs { get; set; }
    public string res { get; set; } = "";
    public string[] streams { get; set; } = [];
}

internal sealed class TileSubscribeAckPayload
{
    public uint subscriptionId { get; set; }
    public string[] accepted { get; set; } = [];
    public string[] rejected { get; set; } = [];
    public int spanMinutes { get; set; }
    public string res { get; set; } = "";
    public int cadenceMs { get; set; }
}

internal sealed class TileUnsubscribeAckPayload
{
    public uint subscriptionId { get; set; }
    public bool removed { get; set; }
}

internal sealed class HistoryExtentRequestPayload
{
    public string wellId { get; set; } = "ga-01";
    public string[] streams { get; set; } = [];
}

internal sealed class HistoryExtentPayload
{
    public string wellId { get; set; } = "ga-01";
    public Dictionary<string, HistoryStreamExtentPayload> streams { get; set; } = [];
    public HistorySharedExtentPayload shared { get; set; } = new();
    public string[] warnings { get; set; } = [];
}

internal class HistoryExtentPayloadBase
{
    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    public long? minTimeMs { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    public long? maxTimeMs { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    public double? minDepth { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    public double? maxDepth { get; set; }
}

internal sealed class HistoryStreamExtentPayload : HistoryExtentPayloadBase;

internal sealed class HistorySharedExtentPayload : HistoryExtentPayloadBase
{
    public string timeMode { get; set; } = "empty";
    public string depthSource { get; set; } = "none";
    public string? warning { get; set; }
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
