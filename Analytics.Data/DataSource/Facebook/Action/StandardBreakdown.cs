namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class StandardBreakdown : FacebookAction
    {
        public string publisher_platform { get; set; }
        public string platform_position { get; set; }
        public string device_platform { get; set; }
        public string impression_device { get; set; }

        public StandardBreakdown(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory) : base(statsReportData, statsReportActions, actionCategory)
        {
            publisher_platform = statsReportData.PublisherPlatform;
            platform_position = statsReportData.PlatformPosition;
            device_platform = statsReportData.DevicePlatform;
            impression_device = statsReportData.ImpressionDevice;
        }
    }
}
