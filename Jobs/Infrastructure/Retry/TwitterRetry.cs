using Greenhouse.Common.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using Polly;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.Jobs.Infrastructure.Retry;

public class TwitterRetry : IRetry
{
    private Policy PollyPolicy { get; set; }
    private Logger Logger { get; set; }

    /// <summary>
    /// Retry Polly that can be cancelled if reaching maxRunTime
    /// </summary>
    /// <param name="jobGuid"></param>
    /// <param name="backOff"></param>
    /// <param name="runtime"></param>
    /// <param name="maxRuntime"></param>
    public TwitterRetry(string jobGuid, IBackOffStrategy backOff, Stopwatch runtime, TimeSpan maxRuntime)
    {
        Logger = NLog.LogManager.GetCurrentClassLogger();

        PollyPolicy = Policy
            .Handle<Exception>()
            .Or<HttpClientProviderRequestException>(exception => exception.StatusCode == HttpStatusCode.TooManyRequests)
            .WaitAndRetry(
                retryCount: backOff.MaxRetry,
                sleepDurationProvider: (retryAttempt, response, context) =>
                {
                    if (response is not HttpClientProviderRequestException exception)
                    {
                        return backOff.GetNextTime();
                    }

                    context["httpRequestExceptionMessage"] = exception.Message;

                    if (exception.Headers is null)
                    {
                        return backOff.GetNextTime();
                    }

                    exception.Headers.TryGetValues("x-rate-limit-reset", out IEnumerable<string> rateLimitResetValues);

                    string rateLimitReset = rateLimitResetValues?.FirstOrDefault();

                    if (string.IsNullOrEmpty(rateLimitReset))
                    {
                        return backOff.GetNextTime();
                    }

                    context["x-rate-limit-reset"] = rateLimitReset;

                    return int.TryParse(rateLimitReset, out int rateLimitResetInSeconds)
                        ? TimeSpan.FromSeconds(rateLimitResetInSeconds - UtilsDate.GetEpochTimeNow())
                        : backOff.GetNextTime();
                },
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    string methodName = "<NOT SPECIFIED>";
                    if (context.TryGetValue("methodName", out object value))
                    {
                        methodName = value.ToString();
                    }

                    if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
                    {
                        //the runtime is greater than the max RunTime
                        string runTimeExceedMessage =
                            $"GetPollyPolicyWithRuntime: The runtime exceeded the allotted time {maxRuntime}";

                        CancellationTokenSource cts = context[nameof(CancellationTokenSource)] as CancellationTokenSource;

                        Logger.Log(Msg.Create(LogLevel.Error, Logger.Name,
                            $"{jobGuid} - STOP POLLY RETRY - {runTimeExceedMessage} - Job error from method: {methodName} with Exception: {exception}. Backoff Policy retry attempt: {retryCount}",
                            exception));

                        cts?.Cancel();

                        throw exception;
                    }

                    // An exception was thrown, retrying
                    Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                        $"{jobGuid} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}",
                        exception));
                }
            );
    }

    public T Execute<T>(Func<T> func)
    {
        CancellationTokenSource contextCancelToken = new();
        Context policyContext = new("RetryContext") { { nameof(CancellationTokenSource), contextCancelToken } };

        PolicyResult<T> policyReturn = PollyPolicy.ExecuteAndCapture(
            (ctx, ct) => func(), policyContext,
            contextCancelToken.Token);

        if (policyReturn.Outcome != OutcomeType.Successful && policyReturn.FinalException != null)
        {
            throw policyReturn.FinalException;
        }

        return policyReturn.Result;
    }
}
