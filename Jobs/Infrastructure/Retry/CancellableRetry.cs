using Greenhouse.Common.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using Polly;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.Jobs.Infrastructure.Retry
{
    internal sealed class CancellableRetry : IRetry
    {
        private Policy PollyPolicy { get; set; }
        private Logger Logger { get; set; }

        /// <summary>
        /// Retry Polly that can be cancelled if reaching maxRunTime
        /// </summary>
        /// <param name="jobGUID"></param>
        /// <param name="backOff"></param>
        /// <param name="runtime"></param>
        /// <param name="maxRunTime"></param>
        public CancellableRetry(string jobGUID, IBackOffStrategy backOff, Stopwatch runtime, TimeSpan maxRunTime)
        {
            Logger = NLog.LogManager.GetCurrentClassLogger();

            PollyPolicy = Policy.Handle<Exception>()
                .WaitAndRetry(backOff.MaxRetry, _ => backOff.GetNextTime(),
                    (exception, currentSleepDuration, retryCount, context) =>
                    {
                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (TimeSpan.Compare(runtime.Elapsed, maxRunTime) == 1)
                        {
                            //the runtime is greater than the max RunTime
                            var runTimeExceedMessage = $"GetPollyPolicyWithRuntime: The runtime exceeded the allotted time {maxRunTime.ToString()}";

                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;

                            Logger.Log(Msg.Create(LogLevel.Error, Logger.Name,
                                $"{jobGUID} STOP POLLY RETRY - {runTimeExceedMessage} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}", exception));
                            cts.Cancel();

                            throw exception;
                        }
                        else
                        {
                            // An exception was thrown, retrying
                            Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                $"{jobGUID} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}|currentSleepDuration:{currentSleepDuration}", exception));
                        }
                    }
                );
        }

        /// <summary>
        /// Retry Polly that can be cancelled if reaching maxRunTime OR has known matching exception
        /// </summary>
        /// <param name="jobGUID"></param>
        /// <param name="backOff"></param>
        /// <param name="knownExceptions"></param>
        /// <param name="runtime"></param>
        /// <param name="maxRunTime"></param>
        public CancellableRetry(string jobGUID, IBackOffStrategy backOff, IEnumerable<string> knownExceptions, Stopwatch runtime, TimeSpan maxRunTime)
        {
            Logger = NLog.LogManager.GetCurrentClassLogger();

            PollyPolicy = Policy.Handle<Exception>()
                .WaitAndRetry(backOff.MaxRetry, _ => backOff.GetNextTime(),
                    (exception, currentSleepDuration, retryCount, context) =>
                    {
                        var matches = knownExceptions.Where(m => exception.Message.Contains(m));

                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (TimeSpan.Compare(runtime.Elapsed, maxRunTime) == 1)
                        {
                            //the runtime is greater than the max RunTime
                            var runTimeExceedMessage = $"GetPollyPolicyWithRuntime: The runtime exceeded the allotted time {maxRunTime.ToString()}";

                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;

                            Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                $"{jobGUID} STOP POLLY RETRY - {runTimeExceedMessage} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}", exception));
                            cts.Cancel();

                            throw exception;
                        }
                        else if (matches.Any())
                        {
                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;
                            string matchingException = string.Join("|", matches);
                            Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                $"{jobGUID} STOP POLLY RETRY - Exception Message(s) matching: {matchingException} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}", exception));
                            context["ExceptionMessages"] = matchingException;
                            cts.Cancel();
                        }
                        else
                        {
                            // An exception was thrown, retrying
                            Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                $"{jobGUID} - CancellableRetry Retry:{retryCount}/{backOff.MaxRetry} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}|currentSleepDuration:{currentSleepDuration}", exception));
                        }
                    }
                );
        }

        public void Execute(Action action)
        {
            var contextCancelToken = new CancellationTokenSource();
            var policyContext = new Context("RetryContext")
            {
                { "CancellationTokenSource", contextCancelToken }
            };

            PollyPolicy.Execute(
                (ctx, ct) => action(), policyContext,
                contextCancelToken.Token);
        }

        public void Execute(Action action, CancellationToken cancellationToken)
        {
            var policyContext = new Context("RetryContext")
            {
                { "CancellationTokenSource", cancellationToken }
            };

            PollyPolicy.Execute(
                (ctx, ct) => action(), policyContext,
                cancellationToken);
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

        public T Execute<T>(Func<T> func, string methodName, Action<Context> actionOnExceptionMatched = null)
        {
            var contextCancelToken = new CancellationTokenSource();
            var policyContext = new Context("RetryContext")
            {
                { "CancellationTokenSource", contextCancelToken },
                { "methodName", methodName },
                { "ExceptionMessages", null}
            };

            var policyReturn = PollyPolicy.ExecuteAndCapture(
                (ctx, ct) => func(), policyContext,
                contextCancelToken.Token);

            if (policyReturn.Outcome != OutcomeType.Successful && policyReturn.FinalException != null)
            {
                if (policyContext["ExceptionMessages"] != null)
                {
                    actionOnExceptionMatched?.Invoke(policyContext);
                }
                else
                {
                    // the exceptions thrown dont match the one(s) expected
                    throw policyReturn.FinalException;
                }
            }

            return policyReturn.Result;
        }
    }
}
