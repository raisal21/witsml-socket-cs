using System.Globalization;
using System.Text.Json.Serialization;
using Npgsql;

namespace WitsmlSocket;

// =============================================================================
// DTOs
// =============================================================================

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
// Resolution picker — validates span × res combo, rejects oversized bin counts
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
    };

    // Max span per resolution — keeps payload bounded (~120-168 bins)
    private static readonly Dictionary<string, TimeSpan> MaxSpan = new()
    {
        ["1s"]  = TimeSpan.FromMinutes(2),
        ["10s"] = TimeSpan.FromMinutes(20),
        ["1m"]  = TimeSpan.FromHours(2),
        ["5m"]  = TimeSpan.FromHours(12),
        ["1h"]  = TimeSpan.FromDays(7),
    };

    public static bool TryGet(string key, out TimeSpan bucket)
        => Resolutions.TryGetValue(key, out bucket);

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
// Query builder + endpoint
// =============================================================================

internal static class TilesController
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/api/tiles", HandleAsync);
    }

    public static async Task<IResult> HandleAsync(
        HttpContext ctx,
        IConfiguration cfg,
        ILogger<Program> log)
    {
        var q = ctx.Request.Query;
        var stream = (q["stream"].ToString() ?? "").Trim().ToLowerInvariant();
        var fromStr = q["from"].ToString();
        var toStr = q["to"].ToString();
        var res = (q["res"].ToString() ?? "").Trim();

        if (stream is not ("drill" or "geo"))
            return Results.BadRequest(new { code = "INVALID_STREAM", message = "stream must be 'drill' or 'geo'" });

        if (!DateTimeOffset.TryParse(fromStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var from))
            return Results.BadRequest(new { code = "INVALID_FROM", message = "from must be ISO8601" });

        if (!DateTimeOffset.TryParse(toStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var to))
            return Results.BadRequest(new { code = "INVALID_TO", message = "to must be ISO8601" });

        if (to <= from)
            return Results.BadRequest(new { code = "INVALID_RANGE", message = "to must be > from" });

        if (!ResolutionPicker.TryGet(res, out _))
            return Results.BadRequest(new { code = "INVALID_RES", message = "res must be one of 1s, 10s, 1m, 5m, 1h" });

        if (!ResolutionPicker.ValidateSpan(res, to - from, out var accepted))
            return Results.BadRequest(new { code = "RES_TOO_FINE", message = "span too large for requested resolution", accepted });

        var connStr = cfg["TimeSeries:QuestDB:PostgresWire"]
                      ?? "Host=localhost;Port=8812;Username=admin;Password=quest;Database=qdb;ServerCompatibilityMode=NoTypeLoading;Timeout=15";

        var (table, traces) = stream == "drill"
            ? ("drill_samples", new[] { "depth", "rpm", "wob", "torque", "hkld", "spp" })
            : ("geo_samples",   new[] { "depth", "gamma", "rop", "h2s", "inc", "azi" });

        // SAMPLE BY interval cannot be parameterized in QuestDB — value is whitelisted above
        var sampleBy = res;
        var aggCols = string.Join(",\n  ",
            traces.SelectMany(t => new[] { $"min({t}) AS {t}_min", $"max({t}) AS {t}_max", $"avg({t}) AS {t}_avg" }));

        var sql = $"""
            SELECT ts,
              {aggCols}
            FROM {table}
            WHERE ts BETWEEN $1 AND $2
            SAMPLE BY {sampleBy} FILL(NONE);
            """;

        var response = new TileResponse
        {
            stream = stream,
            res = res,
            from = from.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
            to = to.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
        };

        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ctx.RequestAborted);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue(from.UtcDateTime);
            cmd.Parameters.AddWithValue(to.UtcDateTime);

            await using var rd = await cmd.ExecuteReaderAsync(ctx.RequestAborted);
            while (await rd.ReadAsync(ctx.RequestAborted))
            {
                var ts = rd.GetDateTime(0);
                var bin = new TileBin
                {
                    ts = DateTime.SpecifyKind(ts, DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                };

                int col = 1;
                foreach (var trace in traces)
                {
                    var stat = new TileStat
                    {
                        min = SafeDouble(rd, col),
                        max = SafeDouble(rd, col + 1),
                        avg = SafeDouble(rd, col + 2),
                    };
                    bin.Traces[trace] = stat;
                    col += 3;
                }
                response.bins.Add(bin);
            }
        }
        catch (NpgsqlException ex)
        {
            log.LogWarning(ex, "[TILES] query failed");
            return Results.Problem(
                detail: ex.Message,
                statusCode: StatusCodes.Status503ServiceUnavailable,
                title: "TSDB unavailable");
        }

        return Results.Json(response, JsonOpts.Default);
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
