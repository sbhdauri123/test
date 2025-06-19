using Greenhouse.Data.DataSource.Innovid;
using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Innovid
{
    public interface IApiClient
    {
        Task<ReportStatusData> GetReportStatusAsync(GetReportStatusOptions options);
        Task<ReportRequest> RequestReportAsync(RequestReportOptions options);
        Task<Stream> DownloadReportAsync(DownloadReportOptions options);
    }
}
