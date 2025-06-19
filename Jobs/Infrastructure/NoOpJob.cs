using Quartz;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Infrastructure
{
    /// <summary>
    /// Minimal implementation o IJob that does not perform any actual work.
    /// </summary>
    public class NoOpJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            return Task.CompletedTask;
        }
    }
}