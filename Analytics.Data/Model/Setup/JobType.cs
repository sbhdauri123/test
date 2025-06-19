using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class JobType : BasePOCO
    {
        [Key]
        public int JobTypeID { get; set; }
        public string JobTypeName { get; set; }
        public int? DefaultSourceJobStepID { get; set; }

        //public int JobCategoryID { get; set; }
        ////FRIDAY 9/8 5:36 PM. Everyone has left for the weekend.

        public string FQClassName { get; set; }
    }
}