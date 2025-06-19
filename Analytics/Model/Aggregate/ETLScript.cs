using Dapper.Contrib.Extensions;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class ETLScript : BasePOCO
    {
        [Dapper.Key]
        public int ETLScriptID { get; set; }
        public int SourceID { get; set; }
        public string ETLScriptName { get; set; }
        public bool IsActive { get; set; }
        public bool IsDefault { get; set; }
        [Computed]
        public string EntityID { get; set; }
    }
}
