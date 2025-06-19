using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class MetadataPartition : BasePOCO
    {
        [Dapper.Key]
        public string Agency { get; set; }

        [Dapper.Key]
        [System.ComponentModel.DataAnnotations.Display(Name = "Table Name")]
        public string TableName { get; set; }

        [System.ComponentModel.DataAnnotations.Display(Name = "Partition Created Date")]
        public DateTime PartitionCreatedDate { get; set; }

        /// <summary>
        /// The last time the job ran.
        /// </summary>
        [System.ComponentModel.DataAnnotations.Display(Name = "Job Run Date")]
        public DateTime JobRunDate { get; set; }

        public string Source { get; set; }
    }
}
