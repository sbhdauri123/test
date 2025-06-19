using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class AdAccountCampaigns
    {
        public string AdAccountID { get; set; }
        public string Timezone { get; set; }
        public List<Campaign> Campaigns { get; set; }
    }

    public class CampaignTZ
    {
        public string AdAccountID { get; set; }
        public string Timezone { get; set; }
        public string CampaignID { get; set; }
        public string Status { get; set; }
        public string StartTime { get; set; }
        public string EndTime { get; set; }
        //Avoids DateTime arithnmetic overflows
        private readonly DateTime MAXDATE = new DateTime(2491, 1, 1);
        private readonly DateTime MINDATE = new DateTime(2000, 1, 1);
        public DateTime StartDateTime { get { return string.IsNullOrEmpty(StartTime) ? MINDATE : DateTime.Parse(StartTime).ToUniversalTime(); } }
        public DateTime EndDateTime { get { return string.IsNullOrEmpty(EndTime) ? MAXDATE : DateTime.Parse(EndTime).ToUniversalTime(); } }
    }
}
