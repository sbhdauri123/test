using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.FRED.Series
{
    public class SeriesState
    {
        [JsonProperty("seriesID")]
        public string SeriesId { get; set; }

        /// <summary>
        /// Last date the daily job downloaded the data
        /// </summary>
        [JsonProperty("deltaDate")]
        public DateTime? DeltaDate { get; set; }
    }
}
