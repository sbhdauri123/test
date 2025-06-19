using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GoogleAds.CampaignDim
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Customer
    {
        public string resourceName { get; set; }
        public string id { get; set; }
        public string descriptiveName { get; set; }
        public string currencyCode { get; set; }
        public string timeZone { get; set; }
    }

    public class NetworkSettings
    {
        public bool targetGoogleSearch { get; set; }
        public bool targetSearchNetwork { get; set; }
        public bool targetContentNetwork { get; set; }
        public bool targetPartnerSearchNetwork { get; set; }
    }

    public class ManualCpv
    {
    }

    public class GeoTargetTypeSetting
    {
        public string positiveGeoTargetType { get; set; }
        public string negativeGeoTargetType { get; set; }
    }

    public class Key
    {
        public string level { get; set; }
        public string timeUnit { get; set; }
        public string eventType { get; set; }
        public int timeLength { get; set; }
    }

    public class FrequencyCap
    {
        public Key key { get; set; }
        public int cap { get; set; }
    }

    public class TargetCpm
    {
    }

    public class Campaign
    {
        public string resourceName { get; set; }
        public string status { get; set; }
        public string adServingOptimizationStatus { get; set; }
        public string advertisingChannelType { get; set; }
        public NetworkSettings networkSettings { get; set; }
        public string experimentType { get; set; }
        public string servingStatus { get; set; }
        public string biddingStrategyType { get; set; }
        public ManualCpv manualCpv { get; set; }
        public string videoBrandSafetySuitability { get; set; }
        public GeoTargetTypeSetting geoTargetTypeSetting { get; set; }
        public string paymentMode { get; set; }
        public string baseCampaign { get; set; }
        public string name { get; set; }
        public string id { get; set; }
        public string campaignBudget { get; set; }
        public string startDate { get; set; }
        public string endDate { get; set; }
        public List<FrequencyCap> frequencyCaps { get; set; }
        public TargetCpm targetCpm { get; set; }
    }

    public class Segments
    {
        public string date { get; set; }
    }

    public class Result
    {
        public Customer customer { get; set; }
        public Campaign campaign { get; set; }
        public Segments segments { get; set; }
    }

    public class Root
    {
        public List<Result> results { get; set; }
        public string fieldMask { get; set; }
    }
}
