using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.TTD.Delta
{
    public class Advertiser
    {
        public string PartnerId { get; set; }
        public string AdvertiserId { get; set; }
        public string AdvertiserName { get; set; }
        public string Description { get; set; }
        public string CurrencyCode { get; set; }
        public string AttributionClickLookbackWindowInSeconds { get; set; }
        public string AttributionImpressionLookbackWindowInSeconds { get; set; }
        public string ClickDedupWindowInSeconds { get; set; }
        public string ConversionDedupWindowInSeconds { get; set; }
        public string DefaultRightMediaOfferTypeId { get; set; }
        public string IndustryCategoryId { get; set; }
        public List<string> Keywords { get; set; }
        public string Availability { get; set; }
        public string LogoURL { get; set; }
        public string DomainAddress { get; set; }
    }

    public class RootAdvertiser
    {
        public List<Advertiser> advertisers { get; set; }
    }
}
