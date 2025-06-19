using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Timezone
    {
        [Key]
        public string TimeZoneName { get; set; }

        public string TimeZone
        {
            get { return TimeZoneName; }
        }
    }
}
