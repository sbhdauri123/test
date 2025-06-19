using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GoogleAds.AdGroupAdMetric
{
    public class Metrics
    {
        public List<string> interactionEventTypes { get; set; }
        public string clicks { get; set; }
        public double videoQuartileP100Rate { get; set; }
        public double videoQuartileP25Rate { get; set; }
        public double videoQuartileP50Rate { get; set; }
        public double videoQuartileP75Rate { get; set; }
        public double videoViewRate { get; set; }
        public string videoViews { get; set; }
        public string viewThroughConversions { get; set; }
        public int conversionsValue { get; set; }
        public int conversions { get; set; }
        public string costMicros { get; set; }
        public int crossDeviceConversions { get; set; }
        public int currentModelAttributedConversions { get; set; }
        public int currentModelAttributedConversionsValue { get; set; }
        public string engagements { get; set; }
        public double activeViewCpm { get; set; }
        public string activeViewImpressions { get; set; }
        public double activeViewMeasurability { get; set; }
        public string activeViewMeasurableCostMicros { get; set; }
        public string activeViewMeasurableImpressions { get; set; }
        public double activeViewViewability { get; set; }
        public int allConversionsValue { get; set; }
        public int allConversions { get; set; }
        public string gmailForwards { get; set; }
        public string gmailSaves { get; set; }
        public string gmailSecondaryClicks { get; set; }
        public string impressions { get; set; }
        public string interactions { get; set; }
    }

    public class Ad
    {
        public string resourceName { get; set; }
    }

    public class AdGroupAd
    {
        public string resourceName { get; set; }
        public Ad ad { get; set; }
    }

    public class Segments
    {
        public string device { get; set; }
        public string date { get; set; }
    }

    public class Result
    {
        public Metrics metrics { get; set; }
        public AdGroupAd adGroupAd { get; set; }
        public Segments segments { get; set; }
    }

    public class Root
    {
        public List<Result> results { get; set; }
        public string fieldMask { get; set; }
    }
}
