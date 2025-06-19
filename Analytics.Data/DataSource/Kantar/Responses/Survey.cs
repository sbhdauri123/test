using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Kantar.Responses
{
    public class SurveyResponse
    {
        [JsonProperty("results")]
        public List<Survey> Surveys { get; set; }
    }

    public class Survey
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("title")]
        public string Title { get; set; }

        [JsonProperty("surveyFamily")]
        public string SurveyFamily { get; set; }

        [JsonProperty("waveName")]
        public string WaveName { get; set; }

        public List<Category> Categories = new();
    }
}
