using Greenhouse.Contracts.Messages;
using Greenhouse.Data.Model.Core;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Infrastructure;

public interface IJobExecutionHandler
{
    Task<bool> TryPublishJobExecutionMessage(ExecuteJob executeJob,
        CancellationToken cancellationToken = default);

    ExecuteJob CreateExecuteJob(JobExecutionDetails jed);
}