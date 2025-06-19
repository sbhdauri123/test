using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.AmazonAdsApi.Responses;

public class ProfileResponse
{
    [JsonProperty("profileId")]
    public string ProfileId { get; set; }

    [JsonProperty("countryCode")]
    public string CountryCode { get; set; }

    [JsonProperty("currencyCode")]
    public string CurrencyCode { get; set; }

    [JsonProperty("timezone")]
    public string Timezone { get; set; }

    [JsonProperty("accountInfo")]
    public AccountInfo AccountInfo { get; set; }
}

public class AccountInfo
{
    [JsonProperty("marketplaceStringId")]
    public string MarketplaceStringId { get; set; }

    [JsonProperty("id")]
    public string Id { get; set; }

    [JsonProperty("type")]
    public string Type { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("validPaymentMethod")]
    public bool ValidPaymentMethod { get; set; }
}
