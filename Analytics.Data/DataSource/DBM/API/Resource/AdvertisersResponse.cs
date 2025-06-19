using Newtonsoft.Json;
using System.Collections.Generic;
namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class GeneralConfig
    {
        [JsonProperty("domainUrl")]
        public string DomainUrl { get; set; }

        [JsonProperty("timeZone")]
        public string TimeZone { get; set; }

        [JsonProperty("currencyCode")]
        public string CurrencyCode { get; set; }
    }

    public class CmHybridConfig
    {
        [JsonProperty("cmAccountId")]
        public string CmAccountId { get; set; }

        [JsonProperty("cmFloodlightConfigId")]
        public string CmFloodlightConfigId { get; set; }

        [JsonProperty("cmSyncableSiteIds")]
        public List<string> CmSyncableSiteIds { get; set; }

        [JsonProperty("dv360ToCmDataSharingEnabled")]
        public bool Dv360ToCmDataSharingEnabled { get; set; }

        [JsonProperty("dv360ToCmCostReportingEnabled")]
        public bool Dv360ToCmCostReportingEnabled { get; set; }

        [JsonProperty("cmFloodlightLinkingAuthorized")]
        public bool CmFloodlightLinkingAuthorized { get; set; }
    }

    public class AdServerConfig
    {
        [JsonProperty("cmHybridConfig")]
        public CmHybridConfig CmHybridConfig { get; set; }
    }

    public class SdfConfig
    {
        [JsonProperty("overridePartnerSdfConfig")]
        public string OverridePartnerSdfConfig { get; set; }

        [JsonProperty("sdfConfig")]
        public SdfConfigData sdfConfig { get; set; }
    }

    public class SdfConfigData
    {
        [JsonProperty("version")]
        public string Version { get; set; }
    }

    public class DataAccessConfig
    {
        [JsonProperty("sdfConfig")]
        public SdfConfig SdfConfig { get; set; }
    }

    public class ServingConfig
    {
        [JsonProperty("exemptTvFromViewabilityTargeting")]
        public bool ExemptTvFromViewabilityTargeting { get; set; }
    }
    public class CreativeConfig
    {
        [JsonProperty("videoCreativeDataSharingAuthorized")]
        public bool VideoCreativeDataSharingAuthorized { get; set; }
    }
    public class IntegrationDetails
    {
        [JsonProperty("integrationCode")]
        public string IntegrationCode { get; set; }
    }
    public class Advertiser
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }

        [JsonProperty("partnerId")]
        public string PartnerId { get; set; }

        [JsonProperty("displayName")]
        public string DisplayName { get; set; }

        [JsonProperty("entityStatus")]
        public string EntityStatus { get; set; }

        [JsonProperty("updateTime")]
        public string UpdateTime { get; set; }

        [JsonProperty("generalConfig")]
        public GeneralConfig GeneralConfig { get; set; }

        [JsonProperty("adServerConfig")]
        public AdServerConfig AdServerConfig { get; set; }

        [JsonProperty("creativeConfig")]
        public CreativeConfig CreativeConfig { get; set; }

        [JsonProperty("dataAccessConfig")]
        public DataAccessConfig DataAccessConfig { get; set; }

        [JsonProperty("integrationDetails")]
        public IntegrationDetails IntegrationDetails { get; set; }

        [JsonProperty("servingConfig")]
        public ServingConfig ServingConfig { get; set; }
    }

    public class AdvertiserResponse
    {
        [JsonProperty("advertisers")]
        public List<Advertiser> Advertisers { get; set; }
    }
}
