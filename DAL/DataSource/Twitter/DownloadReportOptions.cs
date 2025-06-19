using System;

namespace Greenhouse.DAL.DataSource.Twitter;

public record DownloadReportOptions
{
    public string AccountId { get; init; }
    public string ReportUrl { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
        {
            throw new ArgumentException("AccountId is required.", nameof(AccountId));
        }

        if (string.IsNullOrWhiteSpace(ReportUrl))
        {
            throw new ArgumentException("ReportUrl is required.", nameof(ReportUrl));
        }

        if (!Uri.TryCreate(ReportUrl, UriKind.Absolute, out _))
        {
            throw new ArgumentException("ReportUrl must be a valid absolute URL.", nameof(ReportUrl));
        }
    }
}