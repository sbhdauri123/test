using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GoogleAds.AdGroupDim
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class ExplorerAutoOptimizerSetting
    {
        public bool optIn { get; set; }
    }

    public class TargetRestriction
    {
        public string targetingDimension { get; set; }
        public bool bidOnly { get; set; }
    }

    public class TargetingSetting
    {
        public List<TargetRestriction> targetRestrictions { get; set; }
    }

    public class AdGroup
    {
        public string resourceName { get; set; }
        public string status { get; set; }
        public string type { get; set; }
        public ExplorerAutoOptimizerSetting explorerAutoOptimizerSetting { get; set; }
        public string displayCustomBidDimension { get; set; }
        public TargetingSetting targetingSetting { get; set; }
        public string id { get; set; }
        public string name { get; set; }
        public string baseAdGroup { get; set; }
        public string campaign { get; set; }
        public string cpcBidMicros { get; set; }
        public string cpmBidMicros { get; set; }
        public string targetCpaMicros { get; set; }
        public string cpvBidMicros { get; set; }
        public string targetCpmMicros { get; set; }
        public string effectiveTargetCpaMicros { get; set; }
    }

    public class Segments
    {
        public string date { get; set; }
    }

    public class Result
    {
        public AdGroup adGroup { get; set; }
        public Segments segments { get; set; }
    }

    public class Root
    {
        public List<Result> results { get; set; }
        public string fieldMask { get; set; }
    }
}
