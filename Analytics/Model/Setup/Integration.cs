using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Integration : BasePOCO
    {
        [Key]
        public int IntegrationID { get; set; }
        public string IntegrationName { get; set; }
        public string MatchType { get; set; }
        public string RegexMask { get; set; }
        public int SourceID { get; set; }
        public int CredentialID { get; set; }
        public int? InstanceID { get; set; }
        public int MasterClientID { get; set; }
        public int CountryID { get; set; }
        public string EndpointURI { get; set; }
        public string TimeZoneString { get; set; }
        public DateTime FileStartDate { get; set; }
        public bool IsActive { get; set; }

        public bool IsOverrideFailure { get; set; }
        public string ClientSLA { get; set; }

        public bool DisableProcessing { get; set; }
        public int? ParentIntegrationID { get; set; }
        private string AggregateSettingsJson { get; set; }

        private IntegrationAggregateSettings _aggregateSettingsJson;

        public IntegrationAggregateSettings AggregateSettings
        {
            get
            {
                if (string.IsNullOrEmpty(AggregateSettingsJson))
                {
                    return new IntegrationAggregateSettings();
                }

                if (_aggregateSettingsJson == null)
                {
                    _aggregateSettingsJson =
                        Newtonsoft.Json.JsonConvert.DeserializeObject<IntegrationAggregateSettings>(AggregateSettingsJson);
                }

                return _aggregateSettingsJson;
            }
        }
    }
}
