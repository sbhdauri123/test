using System.Collections.Generic;

namespace Greenhouse.Data.Model.LinkedIn
{
    public class ApiResponse
    {
        public IEnumerable<Elements> Elements { get; set; }
        public Metadata Metadata { get; set; }
    }
}
