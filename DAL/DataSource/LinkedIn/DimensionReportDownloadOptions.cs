using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.LinkedIn;

public record DimensionReportDownloadOptions
{
    public string AccountId { get; init; }
    public string DeliveryPath { get; init; }
    public string NextPageToken { get; init; }
    public IEnumerable<string> SearchIds { get; init; } = new List<string>();

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
    }
}

