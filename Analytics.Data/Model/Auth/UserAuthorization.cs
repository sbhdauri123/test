using Dapper;
using System;

namespace Greenhouse.Data.Model.Auth
{
    [Serializable]
    public class UserAuthorization : BasePOCO
    {
        [Key]
        public int UserAuthorizationID { get; set; }
        public string SAMAccountName { get; set; }
        public string Email { get; set; }
        public bool IsAdmin { get; set; }
    }
}