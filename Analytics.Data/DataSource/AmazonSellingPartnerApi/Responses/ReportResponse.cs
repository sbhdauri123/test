using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi;

[Serializable]
public class ReportResponse
{
    [JsonProperty("reportDocumentId")]
    public string ReportDocumentId { get; set; }

    [JsonProperty("compressionalgorithm")]
    public string CompressionAlgorithm { get; set; }

    [JsonProperty("url")]
    public string Url { get; set; }

    [JsonProperty("reportId")]
    public string ReportId { get; set; }

}
