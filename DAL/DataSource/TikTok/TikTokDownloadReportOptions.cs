using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.TikTok;

public record TikTokDownloadReportOptions
{
    public string Endpoint { get; init; }
    public Dictionary<string, string> QueryParameters { get; init; }

    public void Verify()
    {
        if (string.IsNullOrEmpty(Endpoint))
        {
            throw new ArgumentNullException("Endpoint cannot be null or empty.");
        }

        if (QueryParameters.Count == 0)
        {
            throw new ArgumentNullException("QueryParameters cannot be null or empty.");
        }
    }
}
