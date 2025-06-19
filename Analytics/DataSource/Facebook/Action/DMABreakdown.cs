namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class DMABreakdown : FacebookAction
    {
        public string dma { get; set; }

        public DMABreakdown(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory) : base(statsReportData, statsReportActions, actionCategory)
        {
            dma = statsReportData.DMA;
        }
    }
}
