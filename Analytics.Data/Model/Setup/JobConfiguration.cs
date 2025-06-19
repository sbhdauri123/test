using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class JobConfiguration : BasePOCO
    {
        public int JobConfigurationID { get; set; }
        public string JobName { get; set; }
        public string FQClassName { get; set; }
        public int AutoRetryCount { get; set; }
        public int DeferMinutes { get; set; }
        public bool IsActive { get; set; }
    }
}
