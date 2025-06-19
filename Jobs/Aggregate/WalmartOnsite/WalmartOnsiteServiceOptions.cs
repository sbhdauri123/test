using System;

namespace Greenhouse.Jobs.Aggregate.WalmartOnsite
{
    public record WalmartOnsiteServiceOptions
    {
        public string ConsumerId { get; init; }
        public int KeyVersion { get; init; }
        public string PrivateKey { get; init; }
        public string AuthToken { get; init; }
        public string IntegrationEndpointURI { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ConsumerId))
            {
                throw new ArgumentException("ConsumerId is required.", nameof(ConsumerId));
            }

            if (string.IsNullOrWhiteSpace(PrivateKey))
            {
                throw new ArgumentException("PrivateKey is required.", nameof(PrivateKey));
            }

            if (string.IsNullOrWhiteSpace(AuthToken))
            {
                throw new ArgumentException("AuthToken is required.", nameof(AuthToken));
            }

            if (string.IsNullOrWhiteSpace(IntegrationEndpointURI))
            {
                throw new ArgumentException("IntegrationEndpointURI is required.", nameof(IntegrationEndpointURI));
            }

            if (KeyVersion == 0)
            {
                throw new ArgumentException("KeyVersion cannot be 0.", nameof(KeyVersion));
            }

        }
    }
}
