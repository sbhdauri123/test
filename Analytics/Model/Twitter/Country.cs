using Dapper;
using System;

namespace Greenhouse.Data.Model.Twitter
{
    public class Country : BasePOCO
    {
        [Key]
        public string CountryID { get; set; }
        public string CountryName { get; set; }
        new public DateTime CreatedDate { get; set; }
        new public DateTime LastUpdated { get; set; }
    }
}