using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Pinterest
{
    public interface IApiClient
    {
        Task<T> RequestApiReportAsync<T>(RequestApiReportOptions options);
        Task<Stream> DownloadReportAsync(DownloadReportOptions options);
        Task<T> GetReportDownloadUrl<T>(DownloadReportOptions options);
    }
}
