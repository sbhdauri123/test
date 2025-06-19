using Greenhouse.Data.DataSource.DBM.API.Resource;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.DBM.API
{
    [Serializable]
    public class ReportSettings : Greenhouse.Data.DataSource.DCM.ReportSettings
    {
        [JsonProperty("additionalFilters")]
        public IEnumerable<Filter> AdditionalFilters { get; set; }

        [JsonProperty("qs")]
        public string QueryString { get; set; }

        [JsonProperty("callType")]
        public string CallType { get; set; }

        [JsonProperty("pageSize")]
        public string PageSize { get; set; }

        [JsonProperty("fields")]
        public string Fields { get; set; }

        [JsonProperty("orderBy")]
        public string OrderBy { get; set; }

        [JsonProperty("path")]
        public string Path { get; set; }

        [JsonProperty("endpointID")]
        public EndpointID EndpointID { get; set; }

        [JsonProperty("endPath")]
        public string EndPath { get; set; }
    }
}
