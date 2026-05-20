using System.Buffers;
using System.Threading.Channels;

namespace WitsmlSocket;

internal sealed class TelemetryService(
    WebSocketHub hub,
    RigState state,
    AlarmRegistry alarms,
    ChannelWriter<SampleRow> persistence,
    ILogger<TelemetryService> log) : BackgroundService
{
    private uint _seqDrill;
    private uint _seqGeo;
    private long _tick;
    private long _persistDrops;

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
                state.Spp = Jitter(140f, 207f);
                state.Hkld = Jitter(190f, 210f);
                state.Flow = Jitter(1400f, 1600f);

                var drillSeq = _seqDrill++;
                FrameWriter.WriteDrill(drill.AsSpan(0, Constants.DrillFrameBytes), state, drillSeq);
                hub.BroadcastBinary(StreamDef.Drill, drill.AsSpan(0, Constants.DrillFrameBytes));
                Enqueue(new DrillSampleRow(
                    state.TimestampUnixMs, drillSeq,
                    state.Depth, state.Rpm, state.Wob, state.Torque, state.Hkld, state.Spp));

                if (_tick % Constants.GeoTickRatio == 0)
                {
                    state.Gamma = Jitter(40f, 60f);
                    state.Rop = Jitter(2f, 12f);
                    state.H2s = Jitter(0f, 5f);
                    var geoSeq = _seqGeo++;
                    FrameWriter.WriteGeo(geo.AsSpan(0, Constants.GeoFrameBytes), state, geoSeq);
                    hub.BroadcastBinary(StreamDef.Geo, geo.AsSpan(0, Constants.GeoFrameBytes));
                    Enqueue(new GeoSampleRow(
                        state.TimestampUnixMs, geoSeq,
                        state.Depth, state.Gamma, state.Rop, state.H2s, state.Inc, state.Azi));
                }

                MaybeRaiseAlarm();
                _tick++;

                // Log persistence drops every 600 ticks (~60s @ 10Hz) — never block the broadcast hot path
                if (_tick % 600 == 0 && _persistDrops > 0)
                {
                    log.LogWarning("[PERSIST] dropped {N} samples in last window", _persistDrops);
                    _persistDrops = 0;
                }
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

    private void Enqueue(SampleRow row)
    {
        // Non-blocking. On overflow, bounded channel drops oldest — broadcast never blocked.
        if (!persistence.TryWrite(row)) _persistDrops++;
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
                    var idleMs = now - client.LastActivityUtc;
                    if (idleMs > Constants.LivenessTimeoutMs)
                    {
                        log.LogWarning("[PING] {Id} silent for {Ms}ms — terminating", client.ClientId, idleMs);
                        lock (client.StateLock)
                            StateMachine.TryTransition(ref client.State, ClientState.Closing);
                        try { client.Socket.Abort(); } catch { }
                        continue;
                    }

                    if (client.State == ClientState.Active && idleMs > Constants.IdleTimeoutMs)
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
