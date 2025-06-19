using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.DCM
{
    public class DimensionResponse
    {
        public List<string> Data { get; set; } = new List<string>();
        public string NextPageToken { get; set; }
    }
}
