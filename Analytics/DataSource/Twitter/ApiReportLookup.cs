using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Twitter;

public class ApiReportLookup
{
    [JsonProperty("entities")]
    public List<string> EntityIDs { get; set; }
    [JsonProperty("reports")]
    public List<string> OptInReports { get; set; }
}