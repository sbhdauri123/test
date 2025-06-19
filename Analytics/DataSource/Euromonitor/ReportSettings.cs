using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Euromonitor
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
        Category,
        Geography,
        ShareType,
        DataType,
        BrandShare,
        CompanyShare,
        MarketSize,
        JobHistory
    }

    public enum ProcessingStatus
    {
        Queued,
        Processing,
        Processed,
        Completed,
        Failure,
        NoData
    }
}
