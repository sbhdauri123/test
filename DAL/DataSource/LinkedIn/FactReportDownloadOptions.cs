using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.LinkedIn;

public record FactReportDownloadOptions
{
    public string AccountId { get; init; }
    public string DeliveryPath { get; init; }
    public DateTime FileDate { get; init; }
    public IEnumerable<string> ReportFieldNames { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
        {
            throw new ArgumentException("AccountId is required.", nameof(AccountId));
        }

        if (string.IsNullOrWhiteSpace(DeliveryPath))
        {
            throw new ArgumentException("DeliveryPath is required.", nameof(DeliveryPath));
        }

        if (ReportFieldNames is null)
        {
            throw new ArgumentNullException(nameof(ReportFieldNames), "ReportFieldNames is required.");
        }

        if (FileDate == default)
        {
            throw new ArgumentException("FileDate is required and must be a valid date.", nameof(FileDate));
        }
    }
};

