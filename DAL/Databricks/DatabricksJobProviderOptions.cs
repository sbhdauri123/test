using NLog;
using System;

namespace Greenhouse.DAL.Databricks
{
    public record DatabricksJobProviderOptions
    {
        public long IntegrationID { get; init; }
        public long JobLogID { get; init; }
        public int MaxConcurrentJobs { get; init; }
        public int RetryDelayInSeconds { get; init; }
        public string DatabricksJobID { get; init; }
        public Action<LogLevel, string> Logger { get; init; }
        public Action<LogLevel, string, Exception> ExceptionLogger { get; init; }
        public int JobRequestRetryMaxAttempts { get; init; }
        public int JobRequestRetryDelayInSeconds { get; init; }
        public bool JobRequestRetryUseJitter { get; init; } = true;
        public int JobStatusCheckRetryMaxAttempts { get; init; }
        public int JobStatusCheckRetryDelayInSeconds { get; init; }
        public bool JobStatusCheckRetryUseJitter { get; init; } = true;

        public void Validate()
        {
            if (RetryDelayInSeconds is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(RetryDelayInSeconds), RetryDelayInSeconds, "RetryDelayInSeconds must be greater than 0.");
            }

            if (MaxConcurrentJobs is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxConcurrentJobs), MaxConcurrentJobs, "MaxConcurrentJobs must be greater than 0.");
            }

            if (JobRequestRetryMaxAttempts is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(JobRequestRetryMaxAttempts), JobRequestRetryMaxAttempts, "JobRequestRetryMaxAttempts must be greater than 0.");
            }

            if (JobRequestRetryDelayInSeconds is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(JobRequestRetryDelayInSeconds), JobRequestRetryDelayInSeconds, "JobRequestRetryDelayInSeconds must be greater than 0.");
            }

            if (JobStatusCheckRetryMaxAttempts is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(JobStatusCheckRetryMaxAttempts), JobStatusCheckRetryMaxAttempts, "JobStatusCheckRetryMaxAttempts must be greater than 0.");
            }

            if (JobStatusCheckRetryDelayInSeconds is <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(JobStatusCheckRetryDelayInSeconds), JobStatusCheckRetryDelayInSeconds, "JobStatusCheckRetryDelayInSeconds must be greater than 0.");
            }

            if (Logger == null)
            {
                throw new ArgumentNullException(nameof(Logger), "Logger is required.");
            }

            if (ExceptionLogger == null)
            {
                throw new ArgumentNullException(nameof(ExceptionLogger), "ExceptionLogger is required.");
            }

            if (string.IsNullOrWhiteSpace(DatabricksJobID))
            {
                throw new ArgumentException("DatabricksJobID is required.", nameof(DatabricksJobID));
            }
        }
    }
}
