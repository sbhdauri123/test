using Greenhouse.Data.Model.Core;

namespace Greenhouse.Data.Model.Ordered;

public class OrderedQueue : Queue, IOrdered
{
    public int RowNumber { get; set; }
}
