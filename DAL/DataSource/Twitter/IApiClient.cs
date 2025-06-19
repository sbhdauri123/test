using Greenhouse.Data.DataSource.Twitter;
using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Twitter;

public interface IApiClient
{
    Task<ActiveEntities> GetActiveEntitiesAsync(GetActiveEntitiesOptions options);
    Task<ReportRequestStatusResponse> GetReportRequestStatusAsync(GetReportRequestStatusOptions options);
    Task<ReportRequestResponse> GetFactReportAsync(GetFactReportOptions options);
    Task<Stream> DownloadDimensionFileAsync(DownloadDimensionFileOptions options);

    Task<Stream> DownloadReportFileAsync(DownloadReportOptions options);
}