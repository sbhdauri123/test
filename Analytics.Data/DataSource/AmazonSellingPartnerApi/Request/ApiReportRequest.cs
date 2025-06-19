using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using static Greenhouse.Data.DataSource.AmazonSellingPartnerApi.CurrencyCodeHelper;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Request;

[Serializable]
public class ApiReportRequest
{
    public static string PrepareJsonObject(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        if (apiReportSetting.APIReportName.Contains(ReportTypes.VendorSales.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForVendorSalesReport(queueItem, apiReportSetting);
        }
        else if (apiReportSetting.APIReportName.Contains(ReportTypes.CouponPerformance.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForCouponPerformaceReport(queueItem, apiReportSetting);
        }
        else if (apiReportSetting.APIReportName.Contains(ReportTypes.PromotionPerformance.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForPromotionPerformaceReport(queueItem, apiReportSetting);
        }
        else if (apiReportSetting.APIReportName.Contains(ReportTypes.VendorRealTimeSalesReport.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForVendorRealTimeSales(queueItem, apiReportSetting);
        }
        else if (apiReportSetting.APIReportName.Contains(ReportTypes.VendorRealTimeInventoryReport.ToString(), StringComparison.CurrentCultureIgnoreCase)
                || apiReportSetting.APIReportName.Contains(ReportTypes.VendorRealTimeTrafficReport.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForVendorRealTimeInventoryTrafic(queueItem, apiReportSetting);
        }
        else if (apiReportSetting.APIReportName.Contains(ReportTypes.VendorForecasting.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForVendorForecasting(queueItem, apiReportSetting);
        }
        else if (apiReportSetting.APIReportName.Contains(ReportTypes.VendorInventoryReport.ToString(), StringComparison.CurrentCultureIgnoreCase))
        {
            return CreateJsonObjectForVendorSalesReport(queueItem, apiReportSetting);
        }

        return CreateJsonObjectForCommonReport(queueItem, apiReportSetting);
    }

    private static string CreateJsonObjectForCommonReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);

        ReportOptions reportOptions = new ReportOptions();
        string dataStartTime = string.Empty;
        string dataEndTime = string.Empty;
        List<string> marketplaceIds = new List<string>();

        if (reportsetting.UseReportOptions)
        {
            reportOptions.ReportPeriod = reportsetting.ReportPeriod;
            if (string.Equals(reportsetting.ReportPeriod.ToString(), "Week", StringComparison.OrdinalIgnoreCase))
            {
                DateTime date = queueItem.FileDate.AddDays(-1);
                // Find the start and end of the week
                DateTime weekStart = date.Date.AddDays(-(int)date.DayOfWeek);
                DateTime weekEnd = weekStart.AddDays(6);

                dataStartTime = weekStart.ToString("yyyy-MM-dd");
                dataEndTime = weekEnd.ToString("yyyy-MM-dd");
            }
            else
            {
                string formattedDate = queueItem.FileDate.ToString("yyyy-MM-dd");
                dataStartTime = formattedDate;
                dataEndTime = formattedDate;
            }
        }

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequest
        {
            ReportType = reportsetting.ReportType,
            DataStartTime = dataStartTime,
            DataEndTime = dataEndTime,
            MarketplaceIds = marketplaceIds,
            ReportOptions = reportOptions,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    private static string CreateJsonObjectForVendorSalesReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);
        ReportOptionsForVendorSales reportOptions = new ReportOptionsForVendorSales();
        List<string> marketplaceIds = new List<string>();

        if (reportsetting.UseReportOptions)
        {
            reportOptions.ReportPeriod = reportsetting.ReportPeriod;
            reportOptions.DistributorView = reportsetting.DistributorView;
            reportOptions.SellingProgram = reportsetting.SellingProgram;
        }

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequestForVendorSales
        {
            ReportType = reportsetting.ReportType,
            DataStartTime = queueItem.FileDate.ToString("yyyy-MM-dd"),
            DataEndTime = queueItem.FileDate.ToString("yyyy-MM-dd"),
            MarketplaceIds = marketplaceIds,
            ReportOptionsForVendorSales = reportOptions,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    private static string CreateJsonObjectForCouponPerformaceReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);
        ReportOptionsForCouponPerformace reportOptions = new ReportOptionsForCouponPerformace();
        List<string> marketplaceIds = new List<string>();
        var dateRange = GetStartDateAndEndDate(queueItem);

        if (reportsetting.UseReportOptions && reportsetting.UseCampaignDate)
        {
            reportOptions.CampaignStartDateFrom = dateRange.dataStartTime;
            reportOptions.CampaignStartDateTo = dateRange.dataEndTime;
        }

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequestForCouponPerformace
        {
            ReportType = reportsetting.ReportType,
            MarketplaceIds = marketplaceIds,
            ReportOptionsForCouponPerformace = reportOptions,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    private static string CreateJsonObjectForPromotionPerformaceReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);
        ReportOptionsForPromotionPerformace reportOptions = new ReportOptionsForPromotionPerformace();
        List<string> marketplaceIds = new List<string>();

        var dateRange = GetStartDateAndEndDate(queueItem);

        if (reportsetting.UseReportOptions && reportsetting.UsePromotionDate)
        {
            reportOptions.PromotionStartDateFrom = dateRange.dataStartTime;
            reportOptions.PromotionStartDateTo = dateRange.dataEndTime;
        }

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequestForPromotionPerformace
        {
            ReportType = reportsetting.ReportType,
            MarketplaceIds = marketplaceIds,
            ReportOptionsForPromotionPerformace = reportOptions,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    private static string CreateJsonObjectForVendorRealTimeSales(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);
        ReportOptionsForVendorRealTimeSales reportOptions = new ReportOptionsForVendorRealTimeSales();
        List<string> marketplaceIds = new List<string>();

        var dateRange = GetStartDateAndEndDate(queueItem);

        if (reportsetting.UseReportOptions)
        {
            reportOptions.CurrencyCode = MarketplaceManager.GetCurrencyCode(queueItem.EntityID);
        }

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequestForVendorRealTimeSales
        {
            ReportType = reportsetting.ReportType,
            DataStartTime = dateRange.dataStartTime,
            DataEndTime = dateRange.dataEndTime,
            MarketplaceIds = marketplaceIds,
            ReportOptionsForVendorRealTimeSales = reportOptions,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    private static string CreateJsonObjectForVendorRealTimeInventoryTrafic(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);
        List<string> marketplaceIds = new List<string>();
        var dateRange = GetStartDateAndEndDate(queueItem);

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequest
        {
            ReportType = reportsetting.ReportType,
            DataStartTime = dateRange.dataStartTime,
            DataEndTime = dateRange.dataEndTime,
            MarketplaceIds = marketplaceIds,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    private static string CreateJsonObjectForVendorForecasting(OrderedQueue queueItem, APIReport<ReportSettings> apiReportSetting)
    {
        ReportSettings reportsetting = JsonConvert.DeserializeObject<ReportSettings>(apiReportSetting.ReportSettingsJSON);
        ArgumentNullException.ThrowIfNull(reportsetting.ReportType);
        ReportOptionsForVendorForecasting reportOptions = new ReportOptionsForVendorForecasting();
        List<string> marketplaceIds = new List<string>();

        if (reportsetting.UseReportOptions)
        {
            reportOptions.SellingProgram = reportsetting.SellingProgram;
        }

        if (reportsetting.UseMarketplaceIds)
        {
            marketplaceIds.Add(queueItem.EntityID);
        }

        var reportRequest = new ReportRequestForVendorForecasting
        {
            ReportType = reportsetting.ReportType,
            MarketplaceIds = marketplaceIds,
            ReportOptionsForVendorForecasting = reportOptions,
        };

        // Serialize the ReportRequest object to JSON
        return JsonConvert.SerializeObject(reportRequest, Formatting.Indented);
    }

    static (string dataStartTime, string dataEndTime) GetStartDateAndEndDate(OrderedQueue queueItem)
    {
        DateTime inputDate = DateTime.ParseExact(queueItem.FileDate.ToString("yyyy-MM-dd"), "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        // Generate start and end times in UTC
        string dataStartTime = inputDate.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string dataEndTime = inputDate.AddDays(1).AddTicks(-1).ToString("yyyy-MM-ddTHH:mm:ssZ");
        return (dataStartTime, dataEndTime);
    }
}


[Serializable]
public record ReportRequest : BaseReportRequest
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportOptions")]
    public ReportOptions ReportOptions { get; set; }
}

[Serializable]
public record ReportOptions
{
    [JsonProperty("reportPeriod")]
    public string ReportPeriod { get; set; }
}

[Serializable]
public record ReportRequestForVendorSales : BaseReportRequest
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportOptions")]
    public ReportOptionsForVendorSales ReportOptionsForVendorSales { get; set; }
}

[Serializable]
public record ReportOptionsForVendorSales
{
    [JsonProperty("reportPeriod")]
    public string ReportPeriod { get; set; }

    [JsonProperty("distributorView")]
    public string DistributorView { get; set; }

    [JsonProperty("sellingProgram")]
    public string SellingProgram { get; set; }
}

[Serializable]
public record ReportRequestForCouponPerformace
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportOptions")]
    public ReportOptionsForCouponPerformace ReportOptionsForCouponPerformace { get; set; }
}

[Serializable]
public record ReportOptionsForCouponPerformace
{

    [JsonProperty("campaignStartDateFrom")]
    public string CampaignStartDateFrom { get; set; }

    [JsonProperty("campaignStartDateTo")]
    public string CampaignStartDateTo { get; set; }
}

[Serializable]
public record ReportRequestForPromotionPerformace
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportOptions")]
    public ReportOptionsForPromotionPerformace ReportOptionsForPromotionPerformace { get; set; }
}

[Serializable]
public record ReportOptionsForPromotionPerformace
{
    [JsonProperty("promotionStartDateFrom")]
    public string PromotionStartDateFrom { get; set; }

    [JsonProperty("promotionStartDateTo")]
    public string PromotionStartDateTo { get; set; }
}

[Serializable]
public record ReportRequestForVendorRealTimeSales : BaseReportRequest
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportOptions")]
    public ReportOptionsForVendorRealTimeSales ReportOptionsForVendorRealTimeSales { get; set; }
}

[Serializable]
public record ReportOptionsForVendorRealTimeSales
{
    [JsonProperty("currencyCode")]
    public string CurrencyCode { get; set; }
}

[Serializable]
public record ReportRequestForVendorRealTimeInventoryTrafic : BaseReportRequest
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }
}

[Serializable]
public record ReportRequestForVendorForecasting
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("marketplaceIds")]
    public List<string> MarketplaceIds { get; set; }

    [JsonProperty("reportOptions")]
    public ReportOptionsForVendorForecasting ReportOptionsForVendorForecasting { get; set; }
}

[Serializable]
public record ReportOptionsForVendorForecasting
{
    [JsonProperty("sellingProgram")]
    public string SellingProgram { get; set; }
}

