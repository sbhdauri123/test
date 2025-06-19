using Dapper;
using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Source : BasePOCO
    {
        [Key]
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public int DataSourceID { get; set; }
        public int ETLTypeID { get; set; }
        public int BackfillHourOffset { get; set; }
        public int IngestionTypeID { get; set; }
        public bool IsActive { get; set; }
        public bool HasIntegrationJobsChained { get; set; }
        public string ValidationFileName { get; set; }
        public int? DeliveryOffset { get; set; }
        public string PostProcessing { get; set; }
        public string AggregateInitializeSettings { get; set; }
        public bool OverrideEtlScriptName { get; set; }
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

                if (_aggregateProcessingSettings == null)
                {
                    _aggregateProcessingSettings = Newtonsoft.Json.JsonConvert.DeserializeObject<AggregateProcessingSettings>(
                        AggregateProcessingSettingsJson, new JsonSerializerSettings()
                        {

                        });
                }

                return _aggregateProcessingSettings;
            }
        }
    }
}
