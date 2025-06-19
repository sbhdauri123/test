using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Cluster : BasePOCO
    {
        [Key]
        public int ClusterID { get; set; }
        public string ClusterName { get; set; }
        public string AWSRegion { get; set; }
        public string AuthURL { get; set; }
        public string RootS3Bucket { get; set; }
    }
}
