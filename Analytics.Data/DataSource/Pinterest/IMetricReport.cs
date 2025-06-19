using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public interface IMetricReport<T>
    {
        string EntityID { get; set; }

        List<T> DeliveryMetrics { get; set; }
    }
}
