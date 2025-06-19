using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class AggregateInitCacheEntry
    {
        /// <summary>
        /// Entity that had queues created in Aggregate Initialize Job.
        /// </summary>
        [JsonProperty("code")]
        public string APIEntityCode { get; set; }

        /// <summary>
        /// Integration ID to differenciate between the APIEntityCode shared accross different integratin (US/APAC/EMEA)
        /// </summary>
        [JsonProperty("intID")]
        public int IntegrationID { get; set; }

        /// <summary>
        /// MaxFileDate for an entity: max fileDate ever requeted for an APIEntity
        /// if any FileDate requested for that entity is more recent, it means the cache can be cleared and the data requested
        /// </summary>
        [JsonProperty("maxFileDate")]
        public DateTime MaxFileDate { get; set; }
    }
}
