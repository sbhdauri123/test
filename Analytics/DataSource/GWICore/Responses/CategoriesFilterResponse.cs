using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GWICore.Requests
{
    public class CategoriesFilterResponse
    {
        [JsonProperty("categories")]
        public List<CategoryFilter> Categories { get; set; }
    }

    public class CategoryFilter
    {
        [JsonProperty("id")]
        public int Id { get; set; }

        [JsonProperty("datasets")]
        public List<Dataset> Datasets { get; set; }

    }

    public class Dataset
    {
        [JsonProperty("code")]
        public string Code { get; set; }

        [JsonProperty("base_namespace_code")]
        public string BaseNamespaceCode { get; set; }
    }
}
