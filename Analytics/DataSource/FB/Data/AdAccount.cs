using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.FB.Data
{
    public class AdAccount
    {
        /// <summary>
        /// The ID of the ad account
        /// </summary>
        [JsonProperty("account_id")]
        public string AccountId { get; set; }

        /// <summary>
        /// The string act_{ad_account_id}
        /// </summary>
        [JsonProperty("id")]
        public string AccountIdString { get; set; }
    }

    public class AdAccountRow
    {
        public string BusinessManagerEntityID { get; set; }
        public string AccountID { get; set; }

        public string AccountName { get; set; }
    }
}
