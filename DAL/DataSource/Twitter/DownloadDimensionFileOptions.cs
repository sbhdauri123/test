using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Aggregate;
using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.Twitter;

public record DownloadDimensionFileOptions
{
    public string AccountId { get; init; }
    public IEnumerable<string> EntityIds { get; init; } = new List<string>();
    public string Cursor { get; init; }
    public APIReport<ReportSettings> Report { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
        {
            throw new ArgumentException("AccountId is required.", nameof(AccountId));
        }

        if (EntityIds == null)
        {
            throw new ArgumentNullException(nameof(EntityIds), "EntityIds cannot be null.");
        }

        if (Report == null)
        {
            throw new ArgumentNullException(nameof(Report), "Report is required.");
        }
    }
}