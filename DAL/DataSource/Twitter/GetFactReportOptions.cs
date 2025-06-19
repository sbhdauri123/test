using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.DataSource.Twitter;

public record GetFactReportOptions
{
    public string AccountId { get; init; }
    public string Entity { get; init; }
    public string Granularity { get; init; }
    public string Placement { get; init; }
    public string MetricGroups { get; init; }
    public string ReportType { get; init; }
    public string Segmentation { get; init; }
    public string SegmentationType { get; init; }
    public string SegmentationValue { get; init; }
    public DateTime FileDate { get; init; }
    public IEnumerable<string> EntityIds { get; init; } = new List<string>();

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
        {
            throw new ArgumentException("AccountId is required.", nameof(AccountId));
        }

        if (string.IsNullOrWhiteSpace(Entity))
        {
            throw new ArgumentException("Entity is required.", nameof(Entity));
        }

        if (string.IsNullOrWhiteSpace(Granularity))
        {
            throw new ArgumentException("Granularity is required.", nameof(Granularity));
        }

        if (string.IsNullOrWhiteSpace(Placement))
        {
            throw new ArgumentException("Placement is required.", nameof(Placement));
        }

        if (string.IsNullOrWhiteSpace(MetricGroups))
        {
            throw new ArgumentException("MetricGroups is required.", nameof(MetricGroups));
        }

        if (string.IsNullOrWhiteSpace(ReportType))
        {
            throw new ArgumentException("ReportType is required.", nameof(ReportType));
        }

        if (FileDate == default)
        {
            throw new ArgumentException("FileDate is required and must be a valid date.", nameof(FileDate));
        }

        if (!EntityIds.Any())
        {
            throw new ArgumentException("At least one EntityId is required.", nameof(EntityIds));
        }

        if (!string.IsNullOrWhiteSpace(SegmentationType) && string.IsNullOrWhiteSpace(SegmentationValue))
        {
            throw new ArgumentException("SegmentationValue is required when SegmentationType is provided.", nameof(SegmentationValue));
        }

        if (string.IsNullOrWhiteSpace(SegmentationType) && !string.IsNullOrWhiteSpace(SegmentationValue))
        {
            throw new ArgumentException("SegmentationType is required when SegmentationValue is provided.", nameof(SegmentationType));
        }
    }
}