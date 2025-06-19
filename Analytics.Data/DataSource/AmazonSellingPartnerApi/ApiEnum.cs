namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi;

public enum ReportStatus
{
    DONE,
    FATAL
}
public enum InternalReportStatus
{
    ReportCreated,
    COMPLETED,
    FATAL_DUE_TO_UNAVAILABLE_DATA
}

public enum ReportTypes
{
    VendorSales,
    CouponPerformance,
    PromotionPerformance,
    VendorRealTimeInventoryReport,
    VendorRealTimeTrafficReport,
    VendorRealTimeSalesReport,
    VendorForecasting,
    VendorInventoryReport,
}
