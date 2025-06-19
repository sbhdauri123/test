using Greenhouse.Data.DataSource.Snapchat;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;

namespace Greenhouse.DAL.DataSource.Snapchat;

public record GetReportOptions
{
    public string Parameters { get; init; }
    public APIReport<ReportSettings> ApiReport { get; init; }
    public string EntityID { get; init; }
    public string AdAccountID { get; init; }
    public bool HasMappingFile { get; init; }
    public Queue QueueItem { get; set; }
    public bool IsDimension { get; init; }
}
