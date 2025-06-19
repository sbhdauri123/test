using System;
using System.Threading.Tasks;

namespace Greenhouse.QuartzServer.Core
{
    public interface IQuartzServer : IDisposable
    {
        Task InitializeAsync();

        Task StartAsync();

        Task StopAsync();
    }
}
