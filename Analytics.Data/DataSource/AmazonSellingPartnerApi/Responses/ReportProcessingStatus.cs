using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Responses;

public class ReportProcessingStatus
{
    [JsonProperty("processingStatus")]
    public string ProcessingStatus { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportDocumentId")]
    public string ReportDocumentId { get; set; }

    [JsonProperty("reportId")]
    public string ReportId { get; set; }
}
