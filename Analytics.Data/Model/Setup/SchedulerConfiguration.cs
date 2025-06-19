using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class SchedulerConfiguration : BasePOCO
    {
        [Key]
        public int SchedulerConfigurationID { get; set; }
        public string JobExportName { get; set; }
        public string JobName { get; set; }
        public int JobTypeID { get; set; }
        public int ServerID { get; set; }
        public int SortOrder { get; set; }
    }
}
