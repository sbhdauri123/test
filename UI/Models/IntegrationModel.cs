using Greenhouse.Common;
using System.ComponentModel;

namespace Greenhouse.UI.Models
{
    public enum RententionSchedule
    {
        [Description("Last Month")]
        Month1 = 1,
        [Description("Last 2 Months")]
        Month2 = 2,
        [Description("Last 3 Months")]
        Month3 = 3,
        [Description("Last 4 Months")]
        Month4 = 4,
        [Description("Last 5 Months")]
        Month5 = 5,
        [Description("Last 6 Months")]
        Month6 = 6,
        [Description("Last 7 Months")]
        Month7 = 7,
        [Description("Last 8 Months")]
        Month8 = 8,
        [Description("Last 9 Months")]
        Month9 = 9,
        [Description("Last 10 Months")]
        Month10 = 10,
        [Description("Last 11 Months")]
        Month11 = 11,
        [Description("Last 12 Months")]
        Month12 = 12,
    }

    public class IntegrationModel
    {
        ////public List<StallionAgency> Agencies { get; set; }
        //public List<DataItem> CompressionTypes { get; set; }
        //public List<Source> StallionSources { get; set; }
        //public List<Integration> StallionIntegrations { get; set; }
        ////public List<DataLoad> StallionDataLoads { get; set; }
        ////public List<Endpoint> StallionEndpoints { get; set; }
        //public List<DataItem> MasterAccounts { get; set; }
        //public List<DataItem> FileTypes { get; set; }
        //public List<DataItem> DeliveryTypes { get; set; }
        //public List<DataItem> DBs { get; set; }
        //public EMRProc Proc { get; set; }
    }

    public class ProcEditorModel
    {
        public Guid IntegrationGUID { get; set; }
        public string IntegrationName { get; set; }
        public Guid InstanceGUID { get; set; }
        public string InstanceName { get; set; }
        public Guid SourceGUID { get; set; }
        public string SourceName { get; set; }
        public string Environment { get; set; }
    }

    public class DataRetentionModel
    {
        public Guid IntegrationGUID { get; set; }
        public Guid InstanceGUID { get; set; }
        public Guid SourceGUID { get; set; }
        public string SourceName { get; set; }
        public string IntegrationName { get; set; }
        public bool IsDataRetentionChanged { get; set; }
        public float AverageDailyVolume { get; set; }
        public double RetentionID { get; set; }
        public double RetentionEditID { get; set; }
        public float StorageUsed { get; set; }
    }

    public class DataItem
    {
        public string Name { get; set; }
        public string DisplayName { get; set; }
        public int ID { get; set; }
        public int ParentId { get; set; }
        public Guid Guid { get; set; }
    }

    public class TimeZoneModel
    {
        public string ID { get; set; }
        public string Name { get; set; }
    }

    public class DynamoItem
    {
        public string Name { get; set; }
        public string DisplayName { get; set; }
        public Guid ID { get; set; }
    }

    public class S3File
    {
        public string RootBucket { get; set; }
        public string Folder { get; set; }
        public string Name { get; set; }
        public long Size { get; set; }

        public string FullPath
        {
            get
            {
                return string.Format("{0}/{1}/{2}", RootBucket.TrimEnd(Constants.FORWARD_SLASH_ARRAY), Folder.TrimEnd(Constants.FORWARD_SLASH_ARRAY), Name);
            }
        }

        public void Parse(string fullPath)
        {
            if (!string.IsNullOrEmpty(fullPath))
            {
                string fileName = fullPath;
                if (fullPath.Contains(';'))
                {
                    fileName = fullPath.Split(Constants.SEMICOLON_ARRAY)[0];
                    Size = Convert.ToInt64(fullPath.Split(Constants.SEMICOLON_ARRAY)[1]);
                }

                RootBucket = fileName.Substring(0, fileName.IndexOf(Constants.FORWARD_SLASH));
                Name = fileName.Substring(fileName.LastIndexOf(Constants.FORWARD_SLASH) + 1);
                Folder = fileName.Replace(string.Format("{0}/", RootBucket), string.Empty).Replace(Name, string.Empty).TrimEnd(Constants.FORWARD_SLASH_ARRAY);
            }
        }
    }
}