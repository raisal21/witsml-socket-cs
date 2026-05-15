using System.Buffers;

namespace WitsmlSocket;

internal sealed class TelemetryService(WebSocketHub hub, RigState state, AlarmRegistry alarms, ILogger<TelemetryService> log) : BackgroundService
{
    private uint _seqDrill;
    private uint _seqGeo;
    private long _tick;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var drill = ArrayPool<byte>.Shared.Rent(Constants.DrillFrameBytes);
        var geo = ArrayPool<byte>.Shared.Rent(Constants.GeoFrameBytes);
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Constants.TelemetryTickMs));
            while (await timer.WaitForNextTickAsync(ct))
            {
                state.TimestampUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                state.Depth += 0.01f;
                state.Rpm = Jitter(115f, 125f);
                state.Wob = Jitter(18f, 22f);
                state.Torque = Jitter(4f, 6f);
                state.Spp = Jitter(2450f, 2550f);
                state.Hkld = Jitter(190f, 210f);
                state.Flow = Jitter(1400f, 1600f);

                FrameWriter.WriteDrill(drill.AsSpan(0, Constants.DrillFrameBytes), state, _seqDrill++);
                hub.BroadcastBinary(StreamDef.Drill, drill.AsSpan(0, Constants.DrillFrameBytes));

                if (_tick % Constants.GeoTickRatio == 0)
                {
                    state.Gamma = Jitter(40f, 60f);
                    state.Rop = Jitter(2f, 12f);
                    state.H2s = Jitter(0f, 5f);
                    FrameWriter.WriteGeo(geo.AsSpan(0, Constants.GeoFrameBytes), state, _seqGeo++);
                    hub.BroadcastBinary(StreamDef.Geo, geo.AsSpan(0, Constants.GeoFrameBytes));
                }

                MaybeRaiseAlarm();
                _tick++;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { log.LogError(ex, "[TICK] Critical error"); }
        finally
        {
            ArrayPool<byte>.Shared.Return(drill);
            ArrayPool<byte>.Shared.Return(geo);
        }
    }

    private void MaybeRaiseAlarm()
    {
        if (Random.Shared.NextDouble() >= Constants.AlarmRaiseProbability) return;
        state.H2s = Jitter(20f, 50f);
        var alarm = alarms.Raise("HIGH_H2S", "H2S exceeded safety threshold", AlarmSeverity.CRITICAL);
        if (alarm is null) return;
        log.LogInformation("[ALARM RAISED] {Code} ({Id})", alarm.Code, alarm.Id);
        hub.BroadcastJson("ALARM_RAISED", new AlarmEventPayload { alarm = alarm.ToDto() });
    }

    private static float Jitter(float min, float max) => Random.Shared.NextSingle() * (max - min) + min;
}

internal sealed class PingService(WebSocketHub hub, ILogger<PingService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Constants.PingIntervalMs));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                foreach (var (_, client) in hub.Clients)
                {
                    if (!client.IsAlive)
                    {
                        log.LogWarning("[PING] No pong from {Id} — terminating", client.ClientId);
                        lock (client.StateLock)
                            StateMachine.TryTransition(ref client.State, ClientState.Closing);
                        try { client.Socket.Abort(); } catch { }
                        continue;
                    }
                    client.IsAlive = false;

                    if (client.State == ClientState.Active && now - client.LastActivityUtc > Constants.IdleTimeoutMs)
                    {
                        lock (client.StateLock)
                            StateMachine.TryTransition(ref client.State, ClientState.Idle);
                        log.LogInformation("[IDLE] {Id} demoted to IDLE", client.ClientId);
                    }

                    _ = hub.SendJsonAsync<object>(client, "HEARTBEAT", null).AsTask();
                }
            }
        }
        catch (OperationCanceledException) { }
    }
}

internal sealed class AlarmPurgeService(AlarmRegistry alarms, ILogger<AlarmPurgeService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(1));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var n = alarms.PurgeAcknowledgedOlderThan(Constants.AlarmRetention);
                if (n > 0) log.LogDebug("[ALARM] Purged {N} expired alarm(s)", n);
            }
        }
        catch (OperationCanceledException) { }
    }
}
