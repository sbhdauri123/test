namespace Greenhouse.Data.DataSource.AmazonAdsApi;

public enum ReportStatus
{
    QUEUING,
    PROCESSING,
    COMPLETED,
    FAILED
}

public enum InternalReportStatus
{
    INTERNAL_ERROR
}

public enum AccountType
{
    Agency,
    Vendor,
    Seller,
}
