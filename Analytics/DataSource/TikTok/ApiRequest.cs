using Greenhouse.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.TikTok;

[Serializable]
public class ApiReportRequest
{
    public IEnumerable<Model.Aggregate.APIReportField> Dimensions { get; set; }
    public IEnumerable<Model.Aggregate.APIReportField> Metrics { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    private string DateFormat { get; } = "yyyy-MM-dd";
    public bool IsAccountInfo { get; set; }
    public string ReportPath { get; set; }
    public int CurrentPage { get; set; }
    public int PageSize { get; set; }
    public List<string> CampaignIds { get; set; } = new List<string>();
    public ReportSettings ReportSettings { get; set; }
    public string ProfileID { get; set; }
    public string MethodType { get; set; }
    public string ReportToken { get; set; }


    //Flag to indicate if a report was submitted using the async API endpoints
    public bool IsTask { get; set; }

    public string UriPath
    {
        get
        {
            if (IsTask)
            {
                MethodType = System.Net.Http.HttpMethod.Get.ToString();
                return $"{ReportPath}/?advertiser_id={ProfileID}&task_id={ReportToken}";
            }
            else
            {
                var accountString = IsAccountInfo ? $"advertiser_ids=[\"{ProfileID}\"]" : $"advertiser_id={ProfileID}";
                var path = string.IsNullOrEmpty(Parameters) ? $"{ReportPath}/?{accountString}" : $"{ReportPath}/?{accountString}&{Parameters.TrimStart(Constants.AMPERSAND_ARRAY)}";

                if (CurrentPage == 0)
                    return path;

                return $"{path}&page={CurrentPage}";
            }
        }
    }

    private string _parameters;
    public string Parameters
    {
        get
        {
            return _parameters;
        }
    }

    public void SetParameters()
    {
        var parameters = new List<string>();

        if (Metrics != null)
        {
            var metricList = Metrics.Select(x => $"\"{x.APIReportFieldName}\"");
            var metrics = string.Join(",", metricList);
            parameters.Add($"metrics=[{metrics}]");
        }

        if (Dimensions != null)
        {
            var dimensionsList = Dimensions.Select(x => $"\"{x.APIReportFieldName}\"");
            var dimensions = string.Join(",", dimensionsList);
            parameters.Add($"dimensions=[{dimensions}]");
        }

        if (StartDate != null)
        {
            var startDate = StartDate?.ToString(DateFormat);
            parameters.Add($"start_date={startDate}");
        }

        if (EndDate != null)
        {
            var endDate = EndDate?.ToString(DateFormat);
            parameters.Add($"end_date={endDate}");
        }

        if (!string.IsNullOrEmpty(ReportSettings?.Level))
        {
            parameters.Add($"data_level={ReportSettings.Level}");
        }

        if (!string.IsNullOrEmpty(ReportSettings?.ReportType))
        {
            parameters.Add($"report_type={ReportSettings.ReportType}");
        }

        if (ReportSettings?.IsDimensionReport ?? false)
        {
            var filters = new List<string>();

            if (!string.IsNullOrEmpty(ReportSettings.PrimaryStatus))
            {
                filters.Add($"\"primary_status\":\"{ReportSettings.PrimaryStatus}\"");
            }

            if (!string.IsNullOrEmpty(ReportSettings?.SecondaryStatus))
            {
                filters.Add($"\"secondary_status\":\"{ReportSettings.SecondaryStatus}\"");
            }

            if (ReportSettings.BuyingTypes != null && ReportSettings.BuyingTypes.Count != 0)
            {
                var buyingTypes = string.Join(",", ReportSettings.BuyingTypes.Select(x => $"\"{x}\""));
                filters.Add($"\"buying_types\":[{buyingTypes}]");
            }

            if (filters.Count != 0)
            {
                parameters.Add($"filtering={System.Net.WebUtility.UrlEncode($"{{{string.Join(",", filters)}}}")}");
            }
        }

        if (PageSize != 0)
        {
            parameters.Add($"page_size={PageSize}");
        }

        if (CampaignIds.Count != 0 && !(ReportSettings?.IsDimensionReport ?? true))
        {
            var adIds = string.Join(",", CampaignIds.Select(x => x));
            parameters.Add($"filtering=[{{\"field_name\": \"campaign_ids\",\"filter_type\": \"IN\",\"filter_value\": \"[{adIds}]\"}},{{\"field_name\": \"ad_status\",\"filter_type\": \"IN\",\"filter_value\": \"[\\\"STATUS_ALL\\\"]\"}}]");
        }


        _parameters = string.Join("&", parameters);
    }
}
