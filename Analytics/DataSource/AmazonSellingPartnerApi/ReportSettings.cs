using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi;

[Serializable]
public class ReportSettings
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("useMarketplaceIds")]
    public bool UseMarketplaceIds { get; set; }

    [JsonProperty("useReportOptions")]
    public bool UseReportOptions { get; set; }

    [JsonProperty("reportPeriod")]
    public string ReportPeriod { get; set; }

    [JsonProperty("distributorView")]
    public string DistributorView { get; set; }

    [JsonProperty("sellingProgram")]
    public string SellingProgram { get; set; }

    [JsonProperty("useCampaignDate")]
    public bool UseCampaignDate { get; set; }

    [JsonProperty("usePromotionDate")]
    public bool UsePromotionDate { get; set; }
}
