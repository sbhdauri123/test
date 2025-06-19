using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.TTD
{
    public class Creative
    {
        public string creativeid { get; set; }
        public string creativename { get; set; }
        public string description { get; set; }
    }

    public class AdGroup
    {
        public string adgroupid { get; set; }
        public string adgroupname { get; set; }
        public string description { get; set; }
        public string isenabled { get; set; }
        public string availability { get; set; }
        public List<Creative> Creatives { get; set; }
    }

    public class Campaign
    {
        public string campaignid { get; set; }
        public string campaignname { get; set; }
        public string description { get; set; }
        public string startdateutc { get; set; }
        public string enddateutc { get; set; }
        public string availability { get; set; }
        public List<AdGroup> AdGroups { get; set; }
    }

    public class Advertiser
    {
        public string advertiserid { get; set; }
        public string advertisername { get; set; }
        public string description { get; set; }
        public List<Campaign> Campaigns { get; set; }
        public List<Creative> Creatives { get; set; }
    }

    public class PartnerOverview
    {
        public string partnerid { get; set; }
        public string partnername { get; set; }
        public List<Advertiser> Advertisers { get; set; }
    }

    public struct Authentication
    {
        public string token { get; set; }
    }
}