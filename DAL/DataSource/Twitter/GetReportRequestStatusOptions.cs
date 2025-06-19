using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.Twitter;

public record GetReportRequestStatusOptions
{
    public string AccountId { get; init; }
    public IEnumerable<string> JobIds { get; init; } = new List<string>();

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
        {
            throw new ArgumentException("AccountId is required.", nameof(AccountId));
        }

        if (JobIds is null)
        {
            throw new ArgumentNullException(nameof(JobIds), "JobIds is required.");
        }
    }
}