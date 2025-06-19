using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Kantar.Responses
{
    public class CategoryResponse
    {
        [JsonProperty("results")]
        public List<Category> Categories { get; set; }
    }

    public class Category
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("questionid")]
        public string QuestionId { get; set; }

        public TaxonomyType Type { get; set; }

        public string Content { get; set; }

        public string Name => string.IsNullOrEmpty(QuestionId) ? Id : QuestionId;
    }

    public enum TaxonomyType
    {
        Undefined,
        Survey,
        Category,
        SubCategory,
        Question
    }
}