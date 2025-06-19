using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using MassTransit.Testing;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.AmazonAdsApi;

[Serializable]
public class ApiReportRequest
{
    public static string PrepareJsonObject(OrderedQueue queueItem, List<string> advertiserIds, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);

        Configuration config = new Configuration();

        if (reportsetting.UseConfiguration)
        {
            List<Filter> filtersList = new List<Filter>();

            if (reportsetting.UseFilters)
            {
                Filter filter = new Filter
                {
                    Field = reportsetting.Filtersfield,
                    Values = advertiserIds
                };
                filtersList.Add(filter);
                config.Filters = filtersList;
            }
            config.AdProduct = reportsetting.AdProduct;
            config.GroupBy = reportsetting.GroupBy.Split(',').Select(item => item.Trim()).ToList();
            config.ReportTypeId = reportsetting.ReportTypeId;
            config.TimeUnit = reportsetting.TimeUnit;
            config.Format = reportsetting.Format;
            config.Columns = apiReportSetting.ReportFields.Select(x => x.APIReportFieldName).ToList();
        }

        var reportRequest = new ReportRequest
        {
            Name = reportsetting.Name,
            StartDate = queueItem.FileDate.ToString("yyyy-MM-dd"),
            EndDate = queueItem.FileDate.ToString("yyyy-MM-dd"),
            Configuration = config,
        };

        // Serialize the ReportRequest object to JSON
        string json = JsonConvert.SerializeObject(reportRequest, Formatting.Indented);

        return json;
    }
}

[Serializable]
public class ReportRequest
{
    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("startDate")]
    public string StartDate { get; set; }

    [JsonProperty("endDate")]
    public string EndDate { get; set; }

    [JsonProperty("configuration")]
    public Configuration Configuration { get; set; }
}

[Serializable]
public class Configuration
{
    [JsonProperty("adProduct")]
    public string AdProduct { get; set; }

    [JsonProperty("groupBy")]
    public List<string> GroupBy { get; set; }

    [JsonProperty("reportTypeId")]
    public string ReportTypeId { get; set; }

    [JsonProperty("timeUnit")]
    public string TimeUnit { get; set; }

    [JsonProperty("filters")]
    public List<Filter> Filters { get; set; }

    [JsonProperty("format")]
    public string Format { get; set; }

    [JsonProperty("columns")]
    public List<string> Columns { get; set; }
}

[Serializable]
public class Filter
{
    [JsonProperty("field")]
    public string Field { get; set; }

    [JsonProperty("values")]
    public List<string> Values { get; set; }
}


