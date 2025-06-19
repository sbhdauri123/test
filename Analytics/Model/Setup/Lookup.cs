using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Lookup : BasePOCO
    {
        [Key]
        public string Name { get; set; }

        public string Value { get; set; }
        public bool IsEditable { get; set; }
    }
}
