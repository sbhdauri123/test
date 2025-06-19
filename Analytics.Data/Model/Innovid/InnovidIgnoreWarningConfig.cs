using System.Collections.Generic;

namespace Greenhouse.Data.Model.Innovid
{
    public class InnovidIgnoreWarningConfig
    {
        public List<IgnoreWarningSettings> APIEntitiesWithWarningList { get; set; }
    }

    public class IgnoreWarningSettings
    {
        public string ApiEntityCode { get; set; }
    }
}
