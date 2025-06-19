using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Country : BasePOCO
    {
        [Key]

        public int CountryID { get; set; }
        public string CountryName { get; set; }
    }
}
