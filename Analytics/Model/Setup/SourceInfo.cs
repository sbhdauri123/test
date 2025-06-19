using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class SourceInfo
    {
        public int DataSourceID { get; set; }
        public string DataSourceName { get; set; }
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public int? DeliveryOffset { get; set; }
        public int? IntegrationID { get; set; }
        public string IntegrationName { get; set; }
        public int IngestionTypeID;
        public int ETLTypeID { get; set; }
        public bool HasIntegrationJobsChained { get; set; }
        public bool SourceIsActive { get; set; }
        public bool IntegrationIsActive { get; set; }
        public string AggregateProcessingSettingsJson { get; set; }
        private AggregateProcessingSettings _aggregateProcessingSettings;
        public AggregateProcessingSettings AggregateProcessingSettings
        {
            get
            {
                if (string.IsNullOrEmpty(AggregateProcessingSettingsJson))
                {
                    return new AggregateProcessingSettings();
                }

                _aggregateProcessingSettings ??= Newtonsoft.Json.JsonConvert.DeserializeObject<AggregateProcessingSettings>(AggregateProcessingSettingsJson, new JsonSerializerSettings());

                return _aggregateProcessingSettings;
            }
        }
    }
}