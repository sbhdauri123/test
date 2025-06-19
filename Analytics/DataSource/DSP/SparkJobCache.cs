using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.DSP
{
    /// <summary>
    /// Lookup to aid the Spark Job that converts files into Parquet
    /// by retaining the list of failed IDs that should not be resubmitted
    /// until investigated (either Entity or Integration ID)
    /// </summary>
    [Serializable]
    public class SparkJobCache
    {
        [JsonProperty("isIntegrationId")]
        public bool IsIntegrationId { get; set; }
        [JsonProperty("failedIdList")]
        public List<string> FailedIdList { get; set; }
    }
}
