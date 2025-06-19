namespace Greenhouse.Data.DataSource.Facebook.Action
{
    public class FacebookAction
    {
        public string account_id { get; set; }
        public string date_start { get; set; }
        public string action_type { get; set; }
        public string action_video_type { get; set; }
        public string value { get; set; }
        public string action_category { get; set; }
        public string value_1d_view { get; set; }
        public string value_1d_click { get; set; }
        public string value_7d_click { get; set; }
        public string action_reaction { get; set; }

        public FacebookAction(StatsReportData statsReportData, StatsReportActions statsReportActions, string actionCategory)
        {
            account_id = statsReportData.AccountId;
            date_start = statsReportData.DateStart;
            action_type = statsReportActions.ActionType;
            action_video_type = statsReportActions.ActionVideoType;
            value = statsReportActions.Value;
            action_category = actionCategory;
            value_1d_view = statsReportActions.Value1dView;
            value_1d_click = statsReportActions.Value1dClick;
            value_7d_click = statsReportActions.Value7dClick;
            action_reaction = statsReportActions.ActionReaction;
        }
    }
}
