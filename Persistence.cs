using System.Globalization;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

namespace WitsmlSocket;

// =============================================================================
// Sample rows (write DTOs)
// =============================================================================

internal abstract record SampleRow(long TimestampUnixMs, uint Seq);

internal sealed record DrillSampleRow(
    long TimestampUnixMs, uint Seq,
    float Depth, float Rpm, float Wob, float Torque, float Hkld, float Spp, float Flow
) : SampleRow(TimestampUnixMs, Seq);

internal sealed record GeoSampleRow(
    long TimestampUnixMs, uint Seq,
    float Depth, float Gamma, float Rop, float H2s, float Inc, float Azi
) : SampleRow(TimestampUnixMs, Seq);

// =============================================================================
// Store interface
// =============================================================================

internal interface ITimeSeriesStore
{
    Task EnsureSchemaAsync(CancellationToken ct);
    Task WriteBatchAsync(IReadOnlyList<SampleRow> batch, CancellationToken ct);
    Task<int> DropPartitionsOlderThanAsync(int retentionDays, CancellationToken ct);
}

// =============================================================================
// QuestDB store — ILP TCP for writes, HTTP /exec for schema bootstrap
// =============================================================================

internal sealed class QuestDbStore : ITimeSeriesStore, IAsyncDisposable
{
    private readonly ILogger<QuestDbStore> _log;
    private readonly string _ilpHost;
    private readonly int _ilpPort;
    private readonly string _httpBase;
    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public QuestDbStore(IConfiguration cfg, ILogger<QuestDbStore> log)
    {
        _log = log;
        var section = cfg.GetSection("TimeSeries:QuestDB");
        var ilp = section["IlpEndpoint"] ?? "localhost:9009";
        var parts = ilp.Split(':');
        _ilpHost = parts[0];
        _ilpPort = parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 9009;
        _httpBase = section["HttpEndpoint"] ?? "http://localhost:9000";
    }

    public async Task<int> DropPartitionsOlderThanAsync(int retentionDays, CancellationToken ct)
    {
        if (retentionDays < 1)
            throw new ArgumentOutOfRangeException(nameof(retentionDays), "retentionDays must be >= 1 to avoid dropping live data");

        var statements = new[]
        {
            $"ALTER TABLE drill_samples DROP PARTITION WHERE ts < dateadd('d', -{retentionDays}, now())",
            $"ALTER TABLE geo_samples DROP PARTITION WHERE ts < dateadd('d', -{retentionDays}, now())",
        };

        int dropped = 0;
        using var http = new HttpClient { BaseAddress = new Uri(_httpBase) };
        foreach (var sql in statements)
        {
            var url = $"/exec?query={Uri.EscapeDataString(sql)}";
            using var res = await http.GetAsync(url, ct);
            var body = await res.Content.ReadAsStringAsync(ct);

            if (!res.IsSuccessStatusCode)
            {
                // QuestDB returns 400 with "could not remove partition" when no partition matches — treat as no-op
                if (body.Contains("could not remove", StringComparison.OrdinalIgnoreCase) ||
                    body.Contains("no partitions", StringComparison.OrdinalIgnoreCase))
                {
                    _log.LogDebug("[RETENTION] no-op: {Body}", body);
                    continue;
                }
                throw new InvalidOperationException($"DROP PARTITION failed: {res.StatusCode} {body}");
            }
            dropped++;
        }
        return dropped;
    }

    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        var ddl = new[]
        {
            """
            CREATE TABLE IF NOT EXISTS drill_samples (
              ts TIMESTAMP, seq LONG,
              depth FLOAT, rpm FLOAT, wob FLOAT, torque FLOAT, hkld FLOAT, spp FLOAT, flow FLOAT
            ) TIMESTAMP(ts) PARTITION BY DAY WAL;
            """,
            """
            CREATE TABLE IF NOT EXISTS geo_samples (
              ts TIMESTAMP, seq LONG,
              depth FLOAT, gamma FLOAT, rop FLOAT, h2s FLOAT, inc FLOAT, azi FLOAT
            ) TIMESTAMP(ts) PARTITION BY DAY WAL;
            """,
            """
            CREATE TABLE IF NOT EXISTS alarms (
              ts TIMESTAMP, code SYMBOL, severity SYMBOL, message STRING, ack_ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY MONTH;
            """,
        };

        using var http = new HttpClient { BaseAddress = new Uri(_httpBase) };
        foreach (var sql in ddl)
        {
            var url = $"/exec?query={Uri.EscapeDataString(sql)}";
            using var res = await http.GetAsync(url, ct);
            if (!res.IsSuccessStatusCode)
            {
                var body = await res.Content.ReadAsStringAsync(ct);
                throw new InvalidOperationException($"QuestDB schema bootstrap failed: {res.StatusCode} {body}");
            }
        }
        await EnsureOptionalColumnAsync(http, "ALTER TABLE drill_samples ADD COLUMN flow FLOAT", ct);
        _log.LogInformation("[QUESTDB] Schema ready");
    }

    public async Task WriteBatchAsync(IReadOnlyList<SampleRow> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        var sb = new StringBuilder(batch.Count * 128);
        foreach (var row in batch)
        {
            switch (row)
            {
                case DrillSampleRow d: AppendDrill(sb, d); break;
                case GeoSampleRow g:   AppendGeo(sb, g);   break;
            }
        }
        var bytes = Encoding.UTF8.GetBytes(sb.ToString());

        await _writeLock.WaitAsync(ct);
        try
        {
            await EnsureConnectedAsync(ct);
            try
            {
                await _stream!.WriteAsync(bytes, ct);
                await _stream.FlushAsync(ct);
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
                _log.LogWarning(ex, "[QUESTDB] write failed — will reconnect on next batch");
                _stream?.Dispose();
                _tcp?.Dispose();
                _stream = null;
                _tcp = null;
                throw;
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task EnsureConnectedAsync(CancellationToken ct)
    {
        if (_tcp is { Connected: true } && _stream is not null) return;
        _tcp = new TcpClient { NoDelay = true };
        await _tcp.ConnectAsync(_ilpHost, _ilpPort, ct);
        _stream = _tcp.GetStream();
        _log.LogInformation("[QUESTDB] ILP TCP connected {Host}:{Port}", _ilpHost, _ilpPort);
    }

    // ILP line protocol: `table field1=v1,field2=v2 timestamp_ns\n`
    // float fields = bare numeric; int fields suffix `i`. Designated TIMESTAMP at end in ns.
    private static void AppendDrill(StringBuilder sb, DrillSampleRow r)
    {
        var ns = r.TimestampUnixMs * 1_000_000L;
        sb.Append("drill_samples seq=").Append(r.Seq).Append('i');
        AppendField(sb, ",depth=", r.Depth);
        AppendField(sb, ",rpm=",   r.Rpm);
        AppendField(sb, ",wob=",   r.Wob);
        AppendField(sb, ",torque=", r.Torque);
        AppendField(sb, ",hkld=",  r.Hkld);
        AppendField(sb, ",spp=",   r.Spp);
        AppendField(sb, ",flow=",  r.Flow);
        sb.Append(' ').Append(ns).Append('\n');
    }

    private async Task EnsureOptionalColumnAsync(HttpClient http, string sql, CancellationToken ct)
    {
        var url = $"/exec?query={Uri.EscapeDataString(sql)}";
        using var res = await http.GetAsync(url, ct);
        var body = await res.Content.ReadAsStringAsync(ct);
        if (res.IsSuccessStatusCode) return;
        if (body.Contains("already exists", StringComparison.OrdinalIgnoreCase) ||
            body.Contains("duplicate", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }
        throw new InvalidOperationException($"QuestDB schema migration failed: {res.StatusCode} {body}");
    }

    private static void AppendGeo(StringBuilder sb, GeoSampleRow r)
    {
        var ns = r.TimestampUnixMs * 1_000_000L;
        sb.Append("geo_samples seq=").Append(r.Seq).Append('i');
        AppendField(sb, ",depth=", r.Depth);
        AppendField(sb, ",gamma=", r.Gamma);
        AppendField(sb, ",rop=",   r.Rop);
        AppendField(sb, ",h2s=",   r.H2s);
        AppendField(sb, ",inc=",   r.Inc);
        AppendField(sb, ",azi=",   r.Azi);
        sb.Append(' ').Append(ns).Append('\n');
    }

    private static void AppendField(StringBuilder sb, string label, float v)
    {
        sb.Append(label);
        sb.Append(v.ToString("R", CultureInfo.InvariantCulture));
    }

    public async ValueTask DisposeAsync()
    {
        await _writeLock.WaitAsync();
        try
        {
            _stream?.Dispose();
            _tcp?.Dispose();
        }
        finally
        {
            _writeLock.Release();
        }
    }
}

// =============================================================================
// Persistence service — drains bounded channel into store in batches
// =============================================================================

internal sealed class PersistenceService(
    ChannelReader<SampleRow> reader,
    ITimeSeriesStore store,
    ILogger<PersistenceService> log) : BackgroundService
{
    private const int BatchSize = 200;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(100);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // Bootstrap schema with a few retries — QuestDB may not be up yet at host start
        await BootstrapSchemaAsync(ct);

        var batch = new List<SampleRow>(BatchSize);
        using var flushTimer = new PeriodicTimer(FlushInterval);
        long writeFailures = 0;

        while (!ct.IsCancellationRequested)
        {
            while (reader.TryRead(out var row) && batch.Count < BatchSize)
                batch.Add(row);

            try
            {
                await flushTimer.WaitForNextTickAsync(ct);
            }
            catch (OperationCanceledException) { break; }

            if (batch.Count == 0) continue;

            try
            {
                await store.WriteBatchAsync(batch, ct);
                if (writeFailures > 0)
                {
                    log.LogInformation("[PERSIST] write recovered after {N} failures", writeFailures);
                    writeFailures = 0;
                }
            }
            catch (Exception ex)
            {
                writeFailures++;
                if (writeFailures == 1 || writeFailures % 50 == 0)
                    log.LogWarning(ex, "[PERSIST] batch write failed ({N})", writeFailures);
            }
            finally
            {
                batch.Clear();
            }
        }

        // Drain on shutdown — best effort
        while (reader.TryRead(out var row)) batch.Add(row);
        if (batch.Count > 0)
        {
            try { await store.WriteBatchAsync(batch, CancellationToken.None); }
            catch (Exception ex) { log.LogWarning(ex, "[PERSIST] drain write failed"); }
        }
    }

    private async Task BootstrapSchemaAsync(CancellationToken ct)
    {
        for (int i = 0; i < 30 && !ct.IsCancellationRequested; i++)
        {
            try
            {
                await store.EnsureSchemaAsync(ct);
                return;
            }
            catch (Exception ex)
            {
                if (i == 0 || i % 10 == 0)
                    log.LogWarning("[PERSIST] schema bootstrap retry {I}: {Msg}", i, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }
        }
        log.LogError("[PERSIST] schema bootstrap gave up — persistence will keep retrying on writes");
    }
}

// =============================================================================
// Retention job — drops partitions older than RetentionDays, daily
// =============================================================================

internal sealed class RetentionJob(
    ITimeSeriesStore store,
    IConfiguration cfg,
    ILogger<RetentionJob> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var days = cfg.GetValue("TimeSeries:RetentionDays", 31);
        if (days < 1)
        {
            log.LogWarning("[RETENTION] disabled — RetentionDays={Days} (<1)", days);
            return;
        }

        // Initial settle delay so QuestDB and schema bootstrap finish first
        try { await Task.Delay(TimeSpan.FromSeconds(15), ct); }
        catch (OperationCanceledException) { return; }

        await RunOnceAsync(days, ct);

        using var timer = new PeriodicTimer(TimeSpan.FromHours(24));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
                await RunOnceAsync(days, ct);
        }
        catch (OperationCanceledException) { }
    }

    private async Task RunOnceAsync(int days, CancellationToken ct)
    {
        try
        {
            var n = await store.DropPartitionsOlderThanAsync(days, ct);
            log.LogInformation("[RETENTION] dropped {N} table partition(s) older than {Days}d", n, days);
        }
        catch (Exception ex)
        {
            log.LogWarning(ex, "[RETENTION] drop failed — will retry tomorrow");
        }
    }
}
