using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    //Junction class between Source and SourceJobStep
    public class SourceJob : BasePOCO
    {
        [Key]
        public int SourceJobId { get; set; }
        public string SourceJobName { get; set; }
        public int? SourceID { get; set; }
        public int SourceJobStepID { get; set; }
        public int ExecutionTypeID { get; set; }
        public int StepOrderIndex { get; set; }
        public int AutoRetryCount { get; set; }
        public int DeferMinutes { get; set; }

        public SourceJobStep JobStep { get; set; }
    }
}
