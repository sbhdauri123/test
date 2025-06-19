using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class JobCategory : BasePOCO
    {
        [Key]
        public int JobCategoryID { get; set; }
        public string JobCategoryName { get; set; }
    }
}
