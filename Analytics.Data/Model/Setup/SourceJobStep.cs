using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class SourceJobStep : BasePOCO
    {
        //[Key]
        //public int SourceJobStepID { get; set; }
        //public int JobCategoryID { get; set; }
        //public string SourceJobStepName { get; set; }
        //public string Step { get; set; }
        //public string SubType { get; set; }
        //public string ShortDescription { get; set; }
        //public string LongDescription { get; set; }
        //
        //public int IsActive { get; set; }

        [Key]
        public int SourceJobStepID { get; set; }
        public int JobCategoryID { get; set; }
        public int JobTypeID { get; set; }
        public bool IsActive { get; set; }
        public bool IsBatch { get; set; }
        public int IsUnique { get; set; }
        public string SourceJobStepName { get; set; }
        public string Step { get; set; }
        public string SubType { get; set; }
        public string ShortDescription { get; set; }
        public string LongDescription { get; set; }
    }
}