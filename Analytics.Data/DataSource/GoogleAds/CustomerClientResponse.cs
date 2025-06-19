namespace Greenhouse.Data.DataSource.GoogleAds.Aggregate
{
    using Newtonsoft.Json;
    using System.Collections.Generic;

    public partial class CustomerClientResponse
    {
        [JsonProperty("results")]
        public List<CustomerClients> Results { get; set; }

        [JsonProperty("fieldMask")]
        public string FieldMask { get; set; }
    }

    public partial class CustomerClients
    {
        [JsonProperty("customerClient")]
        public CustomerClient CustomerClient { get; set; }
    }

    public partial class CustomerClient
    {
        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        [JsonProperty("level")]

        public string Level { get; set; }

        [JsonProperty("descriptiveName")]
        public string DescriptiveName { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }
    }
}