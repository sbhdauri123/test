using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook.GraphApi.Core
{
    public class GraphData<T> : ApiReportResponse
    {
        public List<T> data { get; set; }
        public Paging paging { get; set; }
    }
}
