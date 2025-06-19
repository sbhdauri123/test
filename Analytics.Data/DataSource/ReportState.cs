using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource;

[Serializable]
public class ReportState
{
    [JsonProperty("dateReportSubmitted")]
    public DateTime DateReportSubmitted { get; set; }

    [JsonProperty("apiEntitiesSubmitted")]
    public HashSet<string> APIEntitiesSubmitted { get; set; }
}
