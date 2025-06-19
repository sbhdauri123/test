using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GoogleAds.AdGroupAdDim
{
    public class InStream
    {
        public string actionButtonLabel { get; set; }
    }

    public class VideoAd
    {
        public InStream inStream { get; set; }
    }

    public class Ad
    {
        public string type { get; set; }
        public VideoAd videoAd { get; set; }
        public string resourceName { get; set; }
        public List<string> finalUrls { get; set; }
        public string trackingUrlTemplate { get; set; }
        public string name { get; set; }
    }

    public class PolicySummary
    {
        public string reviewStatus { get; set; }
        public string approvalStatus { get; set; }
    }

    public class AdGroupAd
    {
        public string resourceName { get; set; }
        public string status { get; set; }
        public Ad ad { get; set; }
        public PolicySummary policySummary { get; set; }
        public string adGroup { get; set; }
    }

    public class Segments
    {
        public string date { get; set; }
    }

    public class Result
    {
        public AdGroupAd adGroupAd { get; set; }
        public Segments segments { get; set; }
    }

    public class Root
    {
        public List<Result> results { get; set; }
        public string fieldMask { get; set; }
    }
}
