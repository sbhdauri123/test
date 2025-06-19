using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.FB.Core
{
    public class ReportFields<T>
    {
        [JsonProperty("data")]
        public List<T> Data { get; set; }

        [JsonProperty("paging")]
        public Paging CursorPaging { get; set; }

        public class Cursors
        {
            [JsonProperty("before")]
            public string Before { get; set; }
            [JsonProperty("after")]
            public string After { get; set; }
        }

        public class Paging
        {
            [JsonProperty("cursors")]
            public Cursors Cursors { get; set; }
            [JsonProperty("next")]
            public string Next { get; set; }
            [JsonProperty("previous")]
            public string Previous { get; set; }
        }
    }
}
