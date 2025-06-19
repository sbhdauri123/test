namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class AdSetAction : StandardBreakdown
    {
        public string campaign_id { get; set; }
        public string adset_id { get; set; }

        public AdSetAction(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory) : base(statsReportData, statsReportActions, actionCategory)
        {
            campaign_id = statsReportData.CampaignId;
            adset_id = statsReportData.AdSetId;
        }
    }
}
