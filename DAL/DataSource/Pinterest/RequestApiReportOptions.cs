using Greenhouse.Data.DataSource.Pinterest;
using Greenhouse.Data.Model.Aggregate;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;

namespace Greenhouse.DAL.DataSource.Pinterest;

public record RequestApiReportOptions
{
    private string _parameters;
    private string _urlExtension;
    public HttpMethod MethodType { get; init; }
    public string ProfileID { get; init; }
    public DateTime? StartDate { get; init; }
    public DateTime? EndDate { get; init; }
    private string DateFormat { get; } = "yyyy-MM-dd";
    public string DeliveryPath { get; init; }
    public IEnumerable<APIReportField> Dimensions { get; set; }
    public IEnumerable<APIReportField> Metrics { get; set; }

    public string UrlExtension
    {
        get
        {
            return $"{_urlExtension.TrimEnd('/')}/{UriPath.TrimEnd('/')}";
        }
        init { _urlExtension = value; }
    }

    public string UriPath
    {
        get
        {

            return $"{ProfileID}/{DeliveryPath}";

        }
    }

    public string Content
    {
        get
        {
            return _parameters;
        }
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(UrlExtension))
        {
            throw new ArgumentException("UrlExtension is required.", nameof(UrlExtension));
        }
        if (MethodType is null)
        {
            throw new ArgumentNullException(nameof(MethodType), "MethodType is required.");
        }
    }

    public void SetParameters(APIReport<ReportSettings> report)
    {
        // v4 changes:
        // introduce body parameters as a json object
        // data_source and tag_version are no longer used

        var parameters = new DeliveryMetricsPayload();

        var columns = new List<string>();
        if (Metrics != null)
        {
            var metricList = Metrics.Select(x => x.APIReportFieldName).ToList();
            columns.AddRange(metricList);
        }

        if (Dimensions != null)
        {
            var entityFieldList = Dimensions.Select(x => x.APIReportFieldName);
            columns.AddRange(entityFieldList);
        }

        if (columns.Count != 0)
            parameters.Columns = columns;

        if (StartDate.HasValue)
        {
            var startDate = StartDate.Value.ToString(DateFormat);
            if (!string.IsNullOrEmpty(startDate))
            {
                parameters.StartDate = startDate;
            }
        }

        if (EndDate.HasValue)
        {
            var endDate = EndDate.Value.ToString(DateFormat);
            if (!string.IsNullOrEmpty(endDate))
            {
                parameters.EndDate = endDate;
            }
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.Level))
        {
            parameters.Level = report.ReportSettings.Level;
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.ClickWindowDays))
        {
            parameters.ClickWindowDays = int.Parse(report.ReportSettings.ClickWindowDays);
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.ConversionReportTime))
        {
            parameters.ConversionReportTime = report.ReportSettings.ConversionReportTime;
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.EngagementWindowDays))
        {
            parameters.EngagementWindowDays = int.Parse(report.ReportSettings.EngagementWindowDays);
        }

        if (report.ReportSettings.Filters != null)
        {
            if (report.ReportSettings.Filters.Count != 0)
            {
                var filterList = report.ReportSettings.Filters.Select(x =>
                new MetricsFilters
                {
                    Field = x.Field,
                    Operator = x.Operator,
                    Values = x.Value.Split(',').Select(val => int.Parse(val)).ToList()
                });

                parameters.MetricsFilters = filterList.ToList();
            }
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.Granularity))
        {
            parameters.Granularity = report.ReportSettings.Granularity;
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.FileFormat))
        {
            parameters.ReportFormat = report.ReportSettings.FileFormat;
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.ViewWindowDays))
        {
            parameters.ViewWindowDays = int.Parse(report.ReportSettings.ViewWindowDays);
        }

        if (report.ReportSettings.TargetingTypes?.Count > 0)
        {
            parameters.TargetingTypes = report.ReportSettings.TargetingTypes;
        }

        if (report.ReportSettings.CampaignStatuses?.Count > 0)
        {
            parameters.CampaignStatuses = report.ReportSettings.CampaignStatuses;
        }

        if (report.ReportSettings.AdGroupStatuses?.Count > 0)
        {
            parameters.AdGroupStatuses = report.ReportSettings.AdGroupStatuses;
        }

        if (report.ReportSettings.AdStatuses?.Count > 0)
        {
            parameters.AdStatuses = report.ReportSettings.AdStatuses;
        }

        _parameters = JsonConvert.SerializeObject(parameters,
            new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            });
    }
}
