using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    [Table("AggDataStatus")]
    public class AggDataStatus : BasePOCO
    {
        public string AdvertiserID { get; set; }
        public string AdvertiserName { get; set; }
        public string FileLogEntityID { get; set; }
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public int DataSourceID { get; set; }
        public string DataSourceName { get; set; }
        public int? IntegrationID { get; set; }
        public string IntegrationName { get; set; }
        public DateTime? ExpectedProcessingDate { get; set; }
        public DateTime? DataDate { get; set; }
        public DateTime? FileDate { get; set; }
        public DateTime? FileLogCreatedDate { get; set; }
        public DateTime? FileLogLastUpdated { get; set; }
        public string ImportStatus { get; set; }
        public string ProcessingStatus { get; set; }
        public string FileGUID { get; set; }
        public string MasterAdvertiserID { get; set; }
        public string MasterAdvertiserName { get; set; }
        public string MasterBusinessUnitID { get; set; }
        public string MasterBusinessUnitName { get; set; }
        public string IntakeFileName { get; set; }
        public bool IsPlaceholder { get; set; }
        public string MasterCountryID { get; set; }
        public string MasterCountryName { get; set; }
        public string MasterAgencyID { get; set; }
        public string MasterAgencyName { get; set; }
        public string ParentID { get; set; }
        public string ParentName { get; set; }
    }
}
