using System.Globalization;
using System.Text.Json.Serialization;
using Npgsql;

namespace WitsmlSocket;

internal sealed class TileResponse
{
    public string stream { get; set; } = "";
    public string res { get; set; } = "";
    public string from { get; set; } = "";
    public string to { get; set; } = "";
    public List<TileBin> bins { get; set; } = [];
}

internal sealed class TileBin
{
    public string ts { get; set; } = "";

    [JsonExtensionData]
    public Dictionary<string, object?> Traces { get; set; } = [];
}

internal sealed class TileStat
{
    public double? min { get; set; }
    public double? max { get; set; }
    public double? avg { get; set; }
}

// =============================================================================
// Resolution picker — protects the tile payload budget before querying QuestDB.
// =============================================================================

internal static class ResolutionPicker
{
    private static readonly Dictionary<string, TimeSpan> Resolutions = new()
    {
        ["1s"]  = TimeSpan.FromSeconds(1),
        ["10s"] = TimeSpan.FromSeconds(10),
        ["1m"]  = TimeSpan.FromMinutes(1),
        ["5m"]  = TimeSpan.FromMinutes(5),
        ["1h"]  = TimeSpan.FromHours(1),
        ["6h"]  = TimeSpan.FromHours(6),
    };

    // Span caps keep every accepted resolution inside the bounded tile budget.
    private static readonly Dictionary<string, TimeSpan> MaxSpan = new()
    {
        ["1s"]  = TimeSpan.FromMinutes(2),
        ["10s"] = TimeSpan.FromMinutes(20),
        ["1m"]  = TimeSpan.FromHours(2),
        ["5m"]  = TimeSpan.FromHours(12),
        ["1h"]  = TimeSpan.FromDays(7),
        ["6h"]  = TimeSpan.FromDays(30),
    };

    public static bool TryGet(string key, out TimeSpan bucket)
        => Resolutions.TryGetValue(key, out bucket);

    public static TileResCode ToCode(string res) => res switch
    {
        "1s" => TileResCode.OneSecond,
        "10s" => TileResCode.TenSeconds,
        "1m" => TileResCode.OneMinute,
        "5m" => TileResCode.FiveMinutes,
        "1h" => TileResCode.OneHour,
        "6h" => TileResCode.SixHours,
        _ => throw new ArgumentOutOfRangeException(nameof(res), res, "unknown tile resolution"),
    };

    public static bool ValidateSpan(string res, TimeSpan span, out string[] accepted)
    {
        accepted = [];
        if (!MaxSpan.TryGetValue(res, out var max))
        {
            accepted = [.. Resolutions.Keys];
            return false;
        }
        if (span > max)
        {
            accepted = MaxSpan
                .Where(kv => kv.Value >= span)
                .Select(kv => kv.Key)
                .ToArray();
            return false;
        }
        return true;
    }
}

// =============================================================================
// Tile queries stay centralized so REST fallback and WS tile frames share shape.
// =============================================================================

internal sealed class TileQueryService(IConfiguration cfg)
{
    private static readonly string[] DrillTraces = ["depth", "rpm", "wob", "torque", "hkld", "spp", "flow"];
    private static readonly string[] GeoTraces = ["depth", "gamma", "rop", "h2s", "inc", "azi"];

    private readonly string _connStr =
        cfg["TimeSeries:QuestDB:PostgresWire"]
        ?? "Host=localhost;Port=8812;Username=admin;Password=quest;Database=qdb;ServerCompatibilityMode=NoTypeLoading;Timeout=15";

    public static bool TryParseStream(string value, out TileStream stream)
    {
        switch ((value ?? "").Trim().ToLowerInvariant())
        {
            case "drill":
                stream = TileStream.Drill;
                return true;
            case "geo":
                stream = TileStream.Geo;
                return true;
            default:
                stream = default;
                return false;
        }
    }

    public static string StreamName(TileStream stream) => stream switch
    {
        TileStream.Drill => "drill",
        TileStream.Geo => "geo",
        _ => throw new ArgumentOutOfRangeException(nameof(stream), stream, "unknown tile stream"),
    };

    public static string[] TraceOrder(TileStream stream) => stream switch
    {
        TileStream.Drill => DrillTraces,
        TileStream.Geo => GeoTraces,
        _ => throw new ArgumentOutOfRangeException(nameof(stream), stream, "unknown tile stream"),
    };

    public static bool IsValidLiveCombo(int spanMinutes, string res, out int cadenceMs)
    {
        cadenceMs = 0;
        if (spanMinutes <= 60) return false;
        if (spanMinutes is 360 or 720 && res == "5m")
        {
            cadenceMs = Constants.TileShortCadenceMs;
            return true;
        }
        if (spanMinutes is 1440 or 4320 or 10080 && res == "1h")
        {
            cadenceMs = Constants.TileLongCadenceMs;
            return true;
        }
        return false;
    }

    public async Task<TileResponse> QueryAsync(TileStream stream, DateTimeOffset from, DateTimeOffset to, string res, CancellationToken ct)
    {
        var (table, traces) = stream switch
        {
            TileStream.Drill => ("drill_samples", DrillTraces),
            TileStream.Geo => ("geo_samples", GeoTraces),
            _ => throw new ArgumentOutOfRangeException(nameof(stream), stream, "unknown tile stream"),
        };

        var aggCols = string.Join(",\n  ",
            traces.SelectMany(t => new[] { $"min({t}) AS {t}_min", $"max({t}) AS {t}_max", $"avg({t}) AS {t}_avg" }));

        var sql = $"""
            SELECT ts,
              {aggCols}
            FROM {table}
            WHERE ts BETWEEN $1 AND $2
            SAMPLE BY {res} FILL(NONE);
            """;

        var response = new TileResponse
        {
            stream = StreamName(stream),
            res = res,
            from = from.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
            to = to.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
        };

        await using var conn = new NpgsqlConnection(_connStr);
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue(from.UtcDateTime);
        cmd.Parameters.AddWithValue(to.UtcDateTime);

        await using var rd = await cmd.ExecuteReaderAsync(ct);
        while (await rd.ReadAsync(ct))
        {
            var ts = rd.GetDateTime(0);
            var bin = new TileBin
            {
                ts = DateTime.SpecifyKind(ts, DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
            };

            int col = 1;
            foreach (var trace in traces)
            {
                bin.Traces[trace] = new TileStat
                {
                    min = SafeDouble(rd, col),
                    max = SafeDouble(rd, col + 1),
                    avg = SafeDouble(rd, col + 2),
                };
                col += 3;
            }
            response.bins.Add(bin);
        }

        return response;
    }

    private static double? SafeDouble(NpgsqlDataReader rd, int ordinal)
    {
        if (rd.IsDBNull(ordinal)) return null;
        var v = rd.GetValue(ordinal);
        return v switch
        {
            double d => d,
            float f  => f,
            decimal m => (double)m,
            long l   => l,
            int i    => i,
            _ => Convert.ToDouble(v, CultureInfo.InvariantCulture),
        };
    }
}

internal sealed class HistoryExtentService(IConfiguration cfg)
{
    private readonly string _connStr =
        cfg["TimeSeries:QuestDB:PostgresWire"]
        ?? "Host=localhost;Port=8812;Username=admin;Password=quest;Database=qdb;ServerCompatibilityMode=NoTypeLoading;Timeout=15";

    public async Task<HistoryExtentPayload> QueryAsync(string wellId, TileStream[] streams, CancellationToken ct)
    {
        var payload = new HistoryExtentPayload
        {
            wellId = string.IsNullOrWhiteSpace(wellId) ? "ga-01" : wellId.Trim(),
        };

        await using var conn = new NpgsqlConnection(_connStr);
        await conn.OpenAsync(ct);

        foreach (var stream in streams)
        {
            payload.streams[TileQueryService.StreamName(stream)] = await QueryStreamAsync(conn, stream, ct);
        }

        payload.shared = BuildShared(payload.streams);
        payload.warnings = payload.shared.warning is null ? [] : [payload.shared.warning];
        return payload;
    }

    private static async Task<HistoryStreamExtentPayload> QueryStreamAsync(
        NpgsqlConnection conn,
        TileStream stream,
        CancellationToken ct)
    {
        var table = stream switch
        {
            TileStream.Drill => "drill_samples",
            TileStream.Geo => "geo_samples",
            _ => throw new ArgumentOutOfRangeException(nameof(stream), stream, "unknown tile stream"),
        };

        var sql = $"""
            SELECT min(ts), max(ts), min(depth), max(depth)
            FROM {table};
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        try
        {
            await using var rd = await cmd.ExecuteReaderAsync(ct);
            if (!await rd.ReadAsync(ct)) return new HistoryStreamExtentPayload();

            return new HistoryStreamExtentPayload
            {
                minTimeMs = SafeUnixMs(rd, 0),
                maxTimeMs = SafeUnixMs(rd, 1),
                minDepth = SafeDouble(rd, 2),
                maxDepth = SafeDouble(rd, 3),
            };
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return new HistoryStreamExtentPayload();
        }
    }

    private static HistorySharedExtentPayload BuildShared(Dictionary<string, HistoryStreamExtentPayload> streams)
    {
        var shared = new HistorySharedExtentPayload();
        var timeExtents = streams.Values
            .Where(e => e.minTimeMs is not null && e.maxTimeMs is not null)
            .ToArray();

        if (timeExtents.Length == 1)
        {
            shared.minTimeMs = timeExtents[0].minTimeMs;
            shared.maxTimeMs = timeExtents[0].maxTimeMs;
            shared.timeMode = "single-stream";
        }
        else if (timeExtents.Length > 1)
        {
            var intersectionMin = timeExtents.Max(e => e.minTimeMs!.Value);
            var intersectionMax = timeExtents.Min(e => e.maxTimeMs!.Value);
            if (intersectionMin <= intersectionMax)
            {
                shared.minTimeMs = intersectionMin;
                shared.maxTimeMs = intersectionMax;
                shared.timeMode = "intersection";
            }
            else
            {
                shared.minTimeMs = timeExtents.Min(e => e.minTimeMs!.Value);
                shared.maxTimeMs = timeExtents.Max(e => e.maxTimeMs!.Value);
                shared.timeMode = "union-no-overlap";
                shared.warning = "STREAM_TIME_RANGES_DO_NOT_OVERLAP";
            }
        }

        if (TryUseDepth(streams, "drill", shared))
        {
            shared.depthSource = "drill";
        }
        else if (TryUseDepth(streams, "geo", shared))
        {
            shared.depthSource = "geo";
        }

        return shared;
    }

    private static bool TryUseDepth(
        Dictionary<string, HistoryStreamExtentPayload> streams,
        string stream,
        HistorySharedExtentPayload shared)
    {
        if (!streams.TryGetValue(stream, out var extent)) return false;
        if (extent.minDepth is null || extent.maxDepth is null) return false;
        shared.minDepth = extent.minDepth;
        shared.maxDepth = extent.maxDepth;
        return true;
    }

    private static long? SafeUnixMs(NpgsqlDataReader rd, int ordinal)
    {
        if (rd.IsDBNull(ordinal)) return null;
        var ts = rd.GetDateTime(ordinal);
        return new DateTimeOffset(DateTime.SpecifyKind(ts, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
    }

    private static double? SafeDouble(NpgsqlDataReader rd, int ordinal)
    {
        if (rd.IsDBNull(ordinal)) return null;
        var v = rd.GetValue(ordinal);
        return v switch
        {
            double d => d,
            float f => f,
            decimal m => (double)m,
            long l => l,
            int i => i,
            _ => Convert.ToDouble(v, CultureInfo.InvariantCulture),
        };
    }
}

internal static class TilesController
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/api/tiles", HandleAsync);
    }

    public static async Task<IResult> HandleAsync(
        HttpContext ctx,
        TileQueryService tiles,
        ILogger<Program> log)
    {
        var q = ctx.Request.Query;
        var stream = (q["stream"].ToString() ?? "").Trim().ToLowerInvariant();
        var fromStr = q["from"].ToString();
        var toStr = q["to"].ToString();
        var res = (q["res"].ToString() ?? "").Trim();

        if (!TileQueryService.TryParseStream(stream, out var tileStream))
            return Results.BadRequest(new { code = "INVALID_STREAM", message = "stream must be 'drill' or 'geo'" });

        if (!DateTimeOffset.TryParse(fromStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var from))
            return Results.BadRequest(new { code = "INVALID_FROM", message = "from must be ISO8601" });

        if (!DateTimeOffset.TryParse(toStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var to))
            return Results.BadRequest(new { code = "INVALID_TO", message = "to must be ISO8601" });

        if (to <= from)
            return Results.BadRequest(new { code = "INVALID_RANGE", message = "to must be > from" });

        if (!ResolutionPicker.TryGet(res, out _))
            return Results.BadRequest(new { code = "INVALID_RES", message = "res must be one of 1s, 10s, 1m, 5m, 1h, 6h" });

        if (!ResolutionPicker.ValidateSpan(res, to - from, out var accepted))
            return Results.BadRequest(new { code = "RES_TOO_FINE", message = "span too large for requested resolution", accepted });

        try
        {
            var response = await tiles.QueryAsync(tileStream, from, to, res, ctx.RequestAborted);
            return Results.Json(response, JsonOpts.Default);
        }
        catch (NpgsqlException ex)
        {
            log.LogWarning(ex, "[TILES] query failed");
            return Results.Problem(
                detail: ex.Message,
                statusCode: StatusCodes.Status503ServiceUnavailable,
                title: "TSDB unavailable");
        }
    }
}
