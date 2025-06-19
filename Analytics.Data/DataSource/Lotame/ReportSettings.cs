using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Lotame
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public ReportType ReportType { get; set; }

        [JsonProperty("endpoint")]
        public string Endpoint { get; set; }

        [JsonProperty("order")]
        public int Order { get; set; }
    }

    public enum ReportType
    {
        Dimension,
        Fact
    }

    public enum ReportName
    {
        FirehoseFeed,
        FirehoseUpdates,
        MappingsFile,
        CookieFile,
        MobileFile,
        HierarchyGrouping,
        HierarchyList,
        HierarchyIdDetail,
        HierarchyNodeDetail,
        BehaviorSearchList,
        BehaviorId,
        BehaviorTypeList
    }
}
