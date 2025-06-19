using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GWICore.Requests
{
    public class CategoryDetailResponse
    {
        [JsonProperty("category")]
        public CategoryDetail CategoryDetail { get; set; }
    }

    public class CategoryDetail
    {
        [JsonProperty("id")]
        public int Id { get; set; }

        [JsonProperty("child_categories")]
        public List<ChildCategory> ChildCategories { get; set; }
    }

    public class ChildCategory
    {
        [JsonProperty("id")]
        public int Id { get; set; }
    }
}
