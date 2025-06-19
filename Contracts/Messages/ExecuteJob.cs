using Greenhouse.Common;

namespace Greenhouse.Contracts.Messages;

public record ExecuteJob
{
    public required string ContractKey { get; init; }
    public required Guid JobGuid { get; init; }
    public required Constants.JobStep Step { get; init; }
    public required int SourceId { get; init; }
    public required int IntegrationId { get; init; }
    public required int ServerId { get; init; }
    public string? TimeZoneString { get; init; }
}