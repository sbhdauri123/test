using Greenhouse.Data.DataSource.LinkedIn;
using Greenhouse.Data.Model.Aggregate;
using System.Collections.Generic;

namespace Greenhouse.Jobs.Aggregate.LinkedIn;

public record Reports
{
    public List<APIReport<ReportSettings>> FactReports { get; set; } = new();
    public APIReport<ReportSettings> AdAccountsReport { get; set; }
    public APIReport<ReportSettings> CampaignGroupsReport { get; set; }
    public APIReport<ReportSettings> CampaignReport { get; set; }
    public APIReport<ReportSettings> CreativesReport { get; set; }
}
