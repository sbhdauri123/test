using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class UserMapping : BasePOCO
    {
        public string MappedName { get; set; }
        public int MappedID { get; set; }
        public string UserID { get; set; }
        public bool IsAdvertiser { get; set; }
        public int DataSourceID { get; set; }
        public string AdvertiserIDS { get; set; }
        public string InstanceIDS { get; set; }
    }
}
