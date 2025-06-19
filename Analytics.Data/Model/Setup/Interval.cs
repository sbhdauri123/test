using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Interval : BasePOCO
    {
        [Key]
        public int IntervalID { get; set; }
        public string IntervalName { get; set; }
    }
}
