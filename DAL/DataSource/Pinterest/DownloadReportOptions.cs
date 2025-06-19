using System;

namespace Greenhouse.DAL.DataSource.Pinterest
{
    public record DownloadReportOptions
    {
        public string UriPath { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(UriPath))
            {
                throw new ArgumentException("UriPath is required.", nameof(UriPath));
            }
        }
    }
}
