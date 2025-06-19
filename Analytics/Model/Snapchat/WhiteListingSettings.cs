using Newtonsoft.Json;
using System.Collections.Generic;


namespace Greenhouse.Data.Model.Snapchat
{
    public class WhiteListingSettings
    {
        [JsonProperty("allowAll")]
        public bool AllowAll { get; set; }

        /// <summary>
        /// Last date the non backfill dimension reports were pulled
        /// </summary>
        [JsonProperty("whiteList")]
        public List<string> WhiteList { get; set; }
    }
}

