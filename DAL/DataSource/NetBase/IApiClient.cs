using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.NetBase
{
    public interface IApiClient
    {
        Task<T> FetchDataAsync<T>(FetchDataOptions options);
        Task<string> FetchRawDataAsync(FetchDataOptions options);
    }
}
