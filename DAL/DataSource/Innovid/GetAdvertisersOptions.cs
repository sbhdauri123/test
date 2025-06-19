using System;

namespace Greenhouse.DAL.DataSource.Innovid
{
    public record GetAdvertisersOptions
    {
        public string UrlExtension { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(UrlExtension))
            {
                throw new ArgumentException("UrlExtension is required.", nameof(UrlExtension));
            }
        }
    }
}
