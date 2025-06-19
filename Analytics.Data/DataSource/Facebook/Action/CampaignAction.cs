namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class CampaignAction : StandardBreakdown
    {
        public string campaign_id { get; set; }

        public CampaignAction(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory) : base(statsReportData, statsReportActions, actionCategory)
        {
            campaign_id = statsReportData.CampaignId;
        }
    }
}
