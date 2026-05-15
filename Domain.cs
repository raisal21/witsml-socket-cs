using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading.Channels;

namespace WitsmlSocket;

internal sealed class RigState
{
    public long TimestampUnixMs;
    public float Depth = 1500f;
    public float Rpm = 120f;
    public float Wob = 20f;
    public float Torque = 5f;
    public float Spp = 2500f;
    public float Hkld = 200f;
    public float Gamma = 50f;
    public float Rop = 25f;
    public float H2s = 10f;
    public float Inc = 0.5f;
    public float Azi = 45f;
    public float Flow = 1500f;
}

internal sealed class Alarm
{
    public required string Id { get; init; }
    public required string Code { get; init; }
    public required string Message { get; init; }
    public required AlarmSeverity Severity { get; init; }
    public long RaisedAt { get; init; }
    public bool Acknowledged { get; set; }
    public AlarmAcknowledgement? AcknowledgedBy { get; set; }

    public AlarmDto ToDto() => new()
    {
        id = Id,
        code = Code,
        message = Message,
        severity = Severity.ToString(),
        raisedAt = RaisedAt,
        acknowledged = Acknowledged,
        acknowledgedBy = AcknowledgedBy,
    };
}

internal sealed class AlarmRegistry
{
    private readonly ConcurrentDictionary<string, Alarm> _alarms = new();
    private readonly ConcurrentDictionary<string, byte> _activeCodes = new();
    private long _sequence;

    public Alarm? Raise(string code, string message, AlarmSeverity severity)
    {
        // dedupe by business code
        if (!_activeCodes.TryAdd(code, 0)) return null;

        var id = $"ALM-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Interlocked.Increment(ref _sequence)}";
        var alarm = new Alarm
        {
            Id = id,
            Code = code,
            Message = message,
            Severity = severity,
            RaisedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };
        _alarms[id] = alarm;
        return alarm;
    }

    public bool TryGet(string id, out Alarm? alarm) => _alarms.TryGetValue(id, out alarm!);

    public bool Acknowledge(string id, string op, string role, out Alarm? alarm)
    {
        if (!_alarms.TryGetValue(id, out alarm)) return false;
        lock (alarm)
        {
            if (alarm.Acknowledged) return false;
            alarm.Acknowledged = true;
            alarm.AcknowledgedBy = new AlarmAcknowledgement
            {
                operatorName = op,
                role = role,
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            };
        }
        _activeCodes.TryRemove(alarm.Code, out _);
        return true;
    }

    public int PurgeAcknowledgedOlderThan(TimeSpan ttl)
    {
        var cutoff = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)ttl.TotalMilliseconds;
        int purged = 0;
        foreach (var (id, alarm) in _alarms)
        {
            if (alarm.Acknowledged && alarm.RaisedAt < cutoff && _alarms.TryRemove(id, out _))
                purged++;
        }
        return purged;
    }
}

internal sealed class ConnectedClient
{
    public required string ClientId { get; set; }
    public required WebSocket Socket { get; init; }
    public required Channel<OutboundFrame> Outbound { get; init; }
    public ClientState State;
    public bool IsAlive = true;
    public long LastActivityUtc;
    public HashSet<StreamDef> Subscriptions { get; } = [];
    public long? SlowSinceUtc;
    public int DroppedFrames;
    public CancellationTokenSource? HandshakeTimer;
    public readonly object StateLock = new();
}

internal readonly record struct OutboundFrame(bool IsBinary, ReadOnlyMemory<byte> Payload);
