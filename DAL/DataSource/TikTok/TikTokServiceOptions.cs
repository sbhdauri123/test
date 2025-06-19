using System;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.TikTok;

public record TikTokServiceOptions
{
    public string HostURI { get; init; }
    public string Version { get; init; }
    public string Token { get; init; }
    public int ThreadSleep { get; init; }
    public int PageSize { get; set; }

    public ParallelOptions ParallelOptions { get; init; }

    public void Verify()
    {
        if (string.IsNullOrEmpty(HostURI))
        {
            throw new ArgumentNullException("HostURI cannot be null or empty.");
        }

        if (string.IsNullOrEmpty(Version))
        {
            throw new ArgumentNullException("Version cannot be null or empty.");
        }

        if (string.IsNullOrEmpty(Token))
        {
            throw new ArgumentNullException("Token cannot be null or empty.");
        }

        if (ThreadSleep <= 0)
        {
            throw new ArgumentNullException("ThreadSleepInSeconds must be greater than 0.");
        }
        if (ParallelOptions == null)
        {
            throw new ArgumentNullException("ParallelOptions cannot be null.");
        }

        if (PageSize <= 0)
        {
            throw new ArgumentNullException("PageSize must be greater than 0.");
        }
    }
}
