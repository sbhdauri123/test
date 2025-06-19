using System;

namespace Greenhouse.DAL.Databricks;

public record DatabricksJobParameterOptions
{
    public string StageFilePath { get; init; }
    public string FileGuid { get; init; }
    public string FileDate { get; init; }
    public string EntityID { get; init; }
    public string EntityName { get; init; }
    public bool IsDimOnly { get; init; }
    public string Profileid { get; init; }
    public string ProfileName { get; init; }
    public string FileCollectionJson { get; init; }
    public string ManifestFilePath { get; init; }

    public int? NoOfConcurrentProcesses { get; init; }
    public int? SourceId { get; init; }
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(StageFilePath))
        {
            throw new ArgumentException("StageFilePath is required.", nameof(StageFilePath));
        }

        if (string.IsNullOrWhiteSpace(FileGuid))
        {
            throw new ArgumentException("FileGuid is required.", nameof(FileGuid));
        }

        if (string.IsNullOrWhiteSpace(FileDate))
        {
            throw new ArgumentException("FileDate is required.", nameof(FileDate));
        }
    }
}
