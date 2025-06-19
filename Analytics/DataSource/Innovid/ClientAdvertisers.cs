using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Innovid
{
    public class ClientData
    {
        [JsonProperty("data")]
        public ClientAdvertiserData Data { get; set; }
        [JsonProperty("status")]
        public string Status { get; set; }
    }

    public class Client
    {
        [JsonProperty("id")]
        public int ClientId { get; set; }
        [JsonProperty("advertisers")]
        public List<Advertiser> Advertisers { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }

    public class ClientAdvertiserData
    {
        [JsonProperty("clients")]
        public List<Client> Clients { get; set; }
    }

    public class Advertiser
    {
        [JsonProperty("id")]
        public int AdvertiserId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}
