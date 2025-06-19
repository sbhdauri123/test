using System.Net;
using System.Net.Http.Headers;

namespace Greenhouse.Data.DataSource.FRED.Core
{
    public class ReportResponse
    {
        public string RawJson { get; set; }
        public HttpResponseHeaders Header { get; set; }
        public HttpStatusCode ResponseCode { get; set; }
    }
}
