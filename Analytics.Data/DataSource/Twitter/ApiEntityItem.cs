using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Twitter;

public class ApiEntityItem
{
    public string Placement { get; set; }
    public List<string> EntityIdList { get; set; }
}