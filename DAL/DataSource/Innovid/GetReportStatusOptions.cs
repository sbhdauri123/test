using System;

namespace Greenhouse.DAL.DataSource.Innovid
{
    public record GetReportStatusOptions
    {
        public string UrlExtension { get; init; }
        public string PropertyName { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(UrlExtension))
            {
                throw new ArgumentException("UrlExtension is required.", nameof(UrlExtension));
            }
        }
    }
}
