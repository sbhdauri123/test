using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Innovid
{
    public class InnovidRequest
    {
        public InnovidRequest()
        {
        }

        public string BaseURI { get; set; }

        public List<KeyValuePair<System.Net.HttpRequestHeader, string>> Headers { get; set; }

        public string Body { get; set; }
    }
}
