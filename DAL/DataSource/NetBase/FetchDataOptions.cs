using System;

namespace Greenhouse.DAL.DataSource.NetBase
{
    public record FetchDataOptions
    {
        public string UrlExtension { get; init; }
        public dynamic ReportParameters { get; init; }
        public string StartDate { get; init; }
        public string EndDate { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(UrlExtension))
            {
                throw new ArgumentException("UrlExtension is required.", nameof(UrlExtension));
            }

            if (ReportParameters == null)
            {
                throw new ArgumentNullException(nameof(ReportParameters), "ReportParameters is required");
            }
        }
    }
}
