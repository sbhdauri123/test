namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class CountryBreakdown : FacebookAction
    {
        public string country { get; set; }
        public string region { get; set; }

        public CountryBreakdown(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory) : base(statsReportData, statsReportActions, actionCategory)
        {
            country = statsReportData.Country;
            region = statsReportData.Region;
        }
    }
}
