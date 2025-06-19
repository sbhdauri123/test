using Greenhouse.Data.DataSource.Innovid;
using System;

namespace Greenhouse.DAL.DataSource.Innovid
{
    public record RequestReportOptions
    {
        public string UrlExtension { get; init; }
        public ReportRequestBody Content { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(UrlExtension))
            {
                throw new ArgumentException("UrlExtension is required.", nameof(UrlExtension));
            }

            if (Content is null)
            {
                throw new ArgumentNullException(nameof(Content), "Content is required.");
            }
        }
    }
}
