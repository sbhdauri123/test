using Greenhouse.Common.Infrastructure;
using Greenhouse.Data.DataSource.Facebook.GraphApi.Core;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using Polly;
using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.Jobs.Infrastructure.Retry
{
    internal sealed class FacebookRetry : IRetry
    {
        private Policy PollyPolicy { get; set; }
        private Logger Logger { get; set; }

        /// <summary>
        /// Retry Polly that can be cancelled if reaching maxRunTime
        /// </summary>
        /// <param name="jobGUID"></param>
        /// <param name="backOff"></param>
        /// <param name="runtime"></param>
        /// <param name="maxRuntime"></param>
        public FacebookRetry(string jobGUID, IBackOffStrategy backOff, Stopwatch runtime, TimeSpan maxRuntime)
        {
            Logger = NLog.LogManager.GetCurrentClassLogger();

            PollyPolicy = Policy.Handle<Exception>().Or<WebException>().Or<FacebookApiThrottleException>()
                .WaitAndRetry(
                    retryCount: backOff.MaxRetry,
                    sleepDurationProvider: (retryAttempt, response, context) =>
                    {
                        if (response is FacebookApiThrottleException)
                        {
                            var retryInSeconds = ((FacebookApiThrottleException)response).RetryAfterInSeconds;
                            if (retryInSeconds > 0)
                            {
                                backOff.Counter++;
                                return TimeSpan.FromSeconds(retryInSeconds);
                            }
                        }

                        return backOff.GetNextTime();
                    },
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
                        {
                            //the runtime is greater than the max RunTime
                            var runTimeExceedMessage =
                                $"GetPollyPolicyWithRuntime: The runtime exceeded the allotted time {maxRuntime.ToString()}";

                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;

                            Logger.Log(Msg.Create(LogLevel.Error, Logger.Name,
                                $"{jobGUID} - STOP POLLY RETRY - {runTimeExceedMessage} - Job error from method: {methodName} with Exception: {exception}. Backoff Policy retry attempt: {retryCount}",
                                exception));
                            cts.Cancel();

                            throw exception;
                        }
                        else
                        {
                            // An exception was thrown, retrying
                            Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                $"{jobGUID} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}",
                                exception));
                        }
                    }
                );
        }

        public T Execute<T>(Func<T> func)
        {
            var contextCancelToken = new CancellationTokenSource();
            var policyContext = new Context("RetryContext")
            {
                { "CancellationTokenSource", contextCancelToken }
            };

            var policyReturn = PollyPolicy.ExecuteAndCapture(
                (ctx, ct) => func(), policyContext,
                contextCancelToken.Token);

            if (policyReturn.Outcome != OutcomeType.Successful && policyReturn.FinalException != null)
            {
                throw policyReturn.FinalException;
            }

            return policyReturn.Result;
        }
    }
}
