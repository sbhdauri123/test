using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Environment : BasePOCO
    {
        [Key]
        public int EnvironmentID { get; set; }
        public string EnvironmentName { get; set; }
    }
}
