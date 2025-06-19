using System.Collections.Generic;
using System.Net;

namespace Greenhouse.Data.DataSource.NetBase.Core
{
    public class ReportResponse
    {
        public string RawJson { get; set; }
        public Dictionary<string, string> Header { get; set; }
        public HttpStatusCode ResponseCode { get; set; }
    }
}
