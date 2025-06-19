using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Responses;

public class CreateReportResponse
{
    [JsonProperty("reportId")]
    public string ReportId { get; set; }
}
