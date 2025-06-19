namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class AdAction : StandardBreakdown
    {
        public string campaign_id { get; set; }
        public string adset_id { get; set; }
        public string ad_id { get; set; }

        public AdAction(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory) : base(statsReportData, statsReportActions, actionCategory)
        {
            campaign_id = statsReportData.CampaignId;
            adset_id = statsReportData.AdSetId;
            ad_id = statsReportData.AdId;
        }
    }
}
