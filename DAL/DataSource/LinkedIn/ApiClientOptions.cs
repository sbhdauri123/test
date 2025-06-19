using System;

namespace Greenhouse.DAL.DataSource.LinkedIn;

public record ApiClientOptions
{
    public string EndpointUri { get; init; }
    public int? PageSize { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(EndpointUri))
        {
            throw new ArgumentException("EndpointUri cannot be empty or whitespace.", nameof(EndpointUri));
        }

        if (!Uri.TryCreate(EndpointUri, UriKind.Absolute, out _))
        {
            throw new UriFormatException($"EndpointUri '{EndpointUri}' is not a valid absolute URI.");
        }

        if (PageSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(PageSize), PageSize, "PageSize must be greater than 0.");
        }
    }
}

