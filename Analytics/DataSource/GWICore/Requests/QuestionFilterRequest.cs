using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GWICore.Requests
{
    /// <summary>
    ///     QuestionFilterRequest.
    /// </summary>
    public class QuestionFilterRequest
    {
        /// <summary>
        ///     Initialize a new instance of the <see cref="QuestionFilterRequest" /> class.
        /// </summary>
        /// <param name="code"></param>
        /// <param name="namespaceCode"></param>
        /// <param name="category"></param>
        /// <param name="dataPoints"></param>
        /// <param name="datasets"></param>
        public QuestionFilterRequest(string code, string namespaceCode, bool category = true, bool dataPoints = true, bool datasets = true)
        {
            Include = new Include
            {
                Categories = category,
                Datapoints = dataPoints,
                Datasets = datasets
            };

            Questions = new List<Question>
            {
                new Question
                {
                    Code = code,
                    NamespaceCode = namespaceCode
                }
            };
        }

        public QuestionFilterRequest(bool category = true, bool datapoints = true, bool datasets = true)
        {
            Include = new Include
            {
                Categories = category,
                Datapoints = datapoints,
                Datasets = datasets
            };
        }

        [JsonProperty("include")]
        public Include Include { get; set; }

        [JsonProperty("questions")]
        public List<Question> Questions { get; set; }
    }

    /// <summary>
    ///     Include.
    /// </summary>
    public class Include
    {
        [JsonProperty("categories")]
        public bool Categories { get; set; }

        [JsonProperty("datapoints")]
        public bool Datapoints { get; set; }

        [JsonProperty("datasets")]
        public bool Datasets { get; set; }
    }

    /// <summary>
    ///     Question.
    /// </summary>
    public class Question
    {
        [JsonProperty("code")]
        public string Code { get; set; }

        [JsonProperty("namespace_code")]
        public string NamespaceCode { get; set; }
    }
}