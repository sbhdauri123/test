using System;

namespace Greenhouse.DAL.DataSource.Twitter;

public record ApiClientOptions
{
    public string EndpointUri { get; init; }
    public string Version { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Version))
        {
            throw new ArgumentException("Version cannot be empty or whitespace.", nameof(Version));
        }

        if (string.IsNullOrWhiteSpace(EndpointUri))
        {
            throw new ArgumentException("EndpointUri cannot be empty or whitespace.", nameof(EndpointUri));
        }

        if (!Uri.TryCreate(EndpointUri, UriKind.Absolute, out _))
        {
            throw new UriFormatException($"EndpointUri '{EndpointUri}' is not a valid absolute URI.");
        }
    }
}