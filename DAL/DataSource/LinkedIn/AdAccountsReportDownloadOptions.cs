using System;

namespace Greenhouse.DAL.DataSource.LinkedIn;

public record AdAccountsReportDownloadOptions
{
    public string AccountId { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
        {
            throw new ArgumentException("AccountId is required.", nameof(AccountId));
        }
    }
}