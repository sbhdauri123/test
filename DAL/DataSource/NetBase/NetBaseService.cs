using Greenhouse.Data.DataSource.NetBase.Core;
using Greenhouse.Data.DataSource.NetBase.Data.MetricValues;
using Greenhouse.Data.DataSource.NetBase.Data.Themes;
using Greenhouse.Data.DataSource.NetBase.Data.Topics;
using Greenhouse.Data.Services;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.DataSource.NetBase
{
    public class NetBaseService
    {
        private int _redshiftMaxLength { get; set; }

        public NetBaseService()
        {
            _redshiftMaxLength = int.Parse(SetupService.GetById<Data.Model.Setup.Lookup>(Common.Constants.LOOKUP_REDSHIFT_MAX_STRING_LENGTH).Value);
        }

        public static void StageMetricSeries(string entityId, ApiReportItem apiReportItem, DateTime fileDate, MetricValuesResponse fullData, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var columnTuple = fullData.Metrics?.SelectMany(m => m.Columns.Select(c => Tuple.Create(m.TimeUnit, c)));
            var setTuple = fullData.Metrics?.SelectMany(m => m.Dataset.SelectMany(d => d.Set.Select(s => Tuple.Create(d.SeriesName, s))));
            var metricValuesData = columnTuple?.Zip(setTuple, (a, b) => Tuple.Create(a, b)).Select(t => new MetricValuesData()
            {
                topic_id = entityId,
                series_name = t.Item2.Item1,
                start_date = fullData.StartDate,
                end_date = fullData.EndDate,
                time_unit = t.Item1.Item1,
                metric_start_date = t.Item1.Item2,
                metric_value = t.Item2.Item2,
                report_type = apiReportItem.ReportType,
                metric_type = apiReportItem.ReportMetric,
                theme_id = apiReportItem.ThemeID
            });

            writeToFileSignature(metricValuesData == null ? new JArray() : JArray.FromObject(metricValuesData), entityId, fileDate, apiReportItem.ReportName);
        }

        public void StageTopics(string entityId, ApiReportItem apiReportItem, DateTime fileDate, List<TopicsResponse> fullData, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var allTopicsData = fullData.Select(x => new TopicsData()
            {
                topic_id = x.TopicId,
                content_type = x.ContentType,
                description = x.Description,
                display_img_url = x.DisplayImgUrl,
                create_date = x.CreateDate,
                edit_date = x.EditDate,
                fh_twitter_status = x.FhTwitterStatus,
                from_date = x.FromDate,
                language_filters = TruncateField(x.LanguageFilters == null ? null : string.Join(",", x.LanguageFilters), _redshiftMaxLength),
                name = x.Name,
                sharing = x.Sharing,
                status = x.Status,
                time_interval = x.TimeInterval,
                to_date = x.ToDate
            });

            writeToFileSignature(JArray.FromObject(allTopicsData), entityId, fileDate, apiReportItem.ReportName);
        }

        public void StageThemes(string entityId, ApiReportItem apiReportItem, DateTime fileDate, List<ThemesResponse> fullData, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var allThemesData = fullData.Select(x => new ThemesData()
            {
                theme_id = x.ThemeId,
                created_date = x.CreatedDate,
                edited_date = x.EditedDate,
                name = x.Name,
                sharing = x.Sharing,
                tags = TruncateField(x.Tags == null ? null : string.Join(",", x.Tags), _redshiftMaxLength)
            });

            writeToFileSignature(JArray.FromObject(allThemesData), entityId, fileDate, apiReportItem.ReportName);
        }

        public static string TruncateField(string value, int maxLength)
        {
            if (string.IsNullOrEmpty(value)) return value;
            return value.Length <= maxLength ? value : value.Substring(0, maxLength);
        }
    }
}
