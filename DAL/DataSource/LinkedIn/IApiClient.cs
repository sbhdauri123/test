using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.LinkedIn;

public interface IApiClient
{
    Task<Stream> DownloadAdAccountsReportStreamAsync(AdAccountsReportDownloadOptions options);
    Task<Stream> DownloadAdCampaignsReportStreamAsync(DimensionReportDownloadOptions options);
    Task<Stream> DownloadAdCampaignGroupsReportStreamAsync(DimensionReportDownloadOptions options);
    Task<Stream> DownloadCreativesReportStreamAsync(DimensionReportDownloadOptions options);
    Task<Stream> DownloadFactReportStreamAsync(FactReportDownloadOptions options);
}

