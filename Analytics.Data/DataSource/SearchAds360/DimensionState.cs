using System;

namespace Greenhouse.Data.DataSource.SearchAds360
{
    public class DimensionState
    {
        public DateTime LatestDateImported { get; set; } = DateTime.MinValue;
        public DateTime DoneLastModified { get; set; } = DateTime.MinValue;
    }
}
