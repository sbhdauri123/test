using Greenhouse.Common;
using System;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class ApiReportRequest
    {
        public bool IsDimension { get; set; }
        public bool IsBackfill { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public string EntityName { get; set; }
        public string EntityId { get; set; }
        public string Endpoint { get; set; }
        public string Parameters { get; set; }
        public string OrganizationID { get; set; }
        public string ParentName { get; set; }
        public string URLPath { get; set; }
        public string AccountID { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Get;

        public string AuthToken { get; set; }
        public static string ContentType => "application/json";

        public string UriPath
        {
            get
            {
                var entityName = EntityName.ToLower();
                if (!IsDimension) return $"{entityName}/{EntityId}/stats";

                //highest call in the chain to retrieve all adaccounts
                if (entityName == "adaccounts")
                {
                    return $"organizations/{OrganizationID}/adaccounts";
                }

                //if this entity has a parent, we will access the entity through all the parent entitiyIds values
                if (!string.IsNullOrEmpty(ParentName))
                {
                    if (!string.IsNullOrEmpty(URLPath))// URLPath takes precedence
                    {
                        return $"{URLPath}/{EntityId}/{entityName}";
                    }

                    return $"{ParentName.ToLower()}/{EntityId}/{entityName}";
                }

                if (!string.IsNullOrEmpty(URLPath))// URLPath takes precedence
                {
                    return $"{URLPath}";
                }

                return $"{entityName}";
            }
        }

        public string GetFullUriPathWithParameters()
        {
            var endpoint = Endpoint.TrimEnd('/').ToLower();
            var parameters = Parameters?.ToLower();
            return string.IsNullOrEmpty(Parameters) ? $"{endpoint}/{UriPath}" : $"{endpoint}/{UriPath}?{Parameters.TrimStart(Constants.AMPERSAND_ARRAY)}";
        }
    }
}
