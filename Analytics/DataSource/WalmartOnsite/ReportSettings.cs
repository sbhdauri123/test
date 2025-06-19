using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.WalmartOnsite;

[Serializable]
public class ReportSettings
{
    [JsonProperty("reportType")]
    public ReportType ReportType { get; set; }

    [JsonProperty("entityType")]
    public string EntityType { get; set; } = "";

    [JsonProperty("fileExtension")]
    public string FileExtension { get; set; }

    [JsonProperty("attributionWindow")]
    public string AttributionWindow { get; set; } = "";

    [JsonProperty("version")]
    public string Version { get; set; } = "v2";

    [JsonProperty("entityTypes")]
    public List<string> EntityTypes { get; set; }

    [JsonProperty("entityStatus")]
    public EntityStatus EntityStatus { get; set; } = EntityStatus.None;
}

public enum ReportType
{
    Report,
    Entity
}

public enum EntityStatus
{
    Enabled,
    Disabled,
    All,
    None
}
