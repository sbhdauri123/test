using System;

namespace Greenhouse.DAL.DataSource.Twitter;

[Serializable]
public record GetActiveEntitiesOptions
{
    public string AccountId { get; init; }
    public string Entity { get; init; }
    public DateTime FileDate { get; init; }

    /// <summary>
    /// Specifies the level of aggregation in which the metrics should be returned.
    /// It's directly aligned with start_time and end_time.
    /// <code> 'DAY': yyyy-MM-dd </code>
    /// <code> default: yyyy-MM-ddTHH:mm:ssZ </code>
    /// </summary>
    public string Granularity { get; init; }

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

        if (FileDate == default)
        {
            throw new ArgumentException("FileDate is required and must be a valid date.", nameof(FileDate));
        }
    }
}