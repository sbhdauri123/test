using Greenhouse.Caching;
using Greenhouse.Common.Extensions;
using Greenhouse.Contracts.Messages;
using Greenhouse.Jobs.Aggregate.LinkedIn;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.JobService.Constants;
using Greenhouse.JobService.Interfaces;
using Greenhouse.Logging;
using Greenhouse.Utilities;

namespace Greenhouse.JobService.Strategies.LinkedIn;

public class JobStrategy : IJobStrategy
{
    private readonly ILogger<JobStrategy> _logger;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly ITokenCache _tokenCache;
    private readonly IJobLoggerFactory _jobLoggerFactory;
    private readonly IEnumerable<IDragoJob> _jobs;

    public string ContractKey => ContractKeys.LinkedInAggregateImportJob;

    public JobStrategy(ILogger<JobStrategy> logger, IHttpClientProvider httpClientProvider,
        ITokenCache tokenCache, IJobLoggerFactory jobLoggerFactory, IEnumerable<IDragoJob> jobs)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(httpClientProvider);
        ArgumentNullException.ThrowIfNull(tokenCache);
        ArgumentNullException.ThrowIfNull(jobLoggerFactory);
        ArgumentNullException.ThrowIfNull(jobs);

        _logger = logger;
        _httpClientProvider = httpClientProvider;
        _tokenCache = tokenCache;
        _jobLoggerFactory = jobLoggerFactory;
        _jobs = jobs;
    }

    public async Task ExecuteAsync(ExecuteJob executeJob, CancellationToken cancellationToken = default)
    {
        _logger.LogMessage(LogLevel.Debug, "--> Starting LinkedIn Job Strategy...");

        IDragoJob dragoJob = _jobs.OfType<ImportJob>().FirstOrDefault()
                             ?? throw new InvalidOperationException("LinkedIn Import Job not found");

        ImportJob job = (ImportJob)dragoJob;
        bool canAutoRetry = false;
        try
        {
            job.HttpClientProvider = _httpClientProvider;
            job.TokenCache = _tokenCache;
            job.JobLogger = _jobLoggerFactory.GetJobLogger();

            await job.Initialize(executeJob.SourceId, executeJob.IntegrationId, executeJob.ServerId);
            job.JobLogger.Start();
            job.PreExecute();
            job.Execute();
            job.JobLogger.Finish();
            job.PostExecute();
        }
        catch (Exception e)
        {
            job.JobLogger.LogException(e);
            canAutoRetry = true;
        }
        finally
        {
            job.Complete();

            if (canAutoRetry)
            {
                job.RetryJob();
            }

            job.Dispose();
        }

        _logger.LogMessage(LogLevel.Debug, "--> Finished LinkedIn Job Strategy.");
    }
}