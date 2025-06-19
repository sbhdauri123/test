using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Skai.AsyncReport
{
    [Serializable]
    public class AsyncStatusResponse
    {
        [JsonProperty("status")]
        public string Status { get; set; }
    }
}