using Greenhouse.Common.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using Polly;
using Polly.Retry;
using System;
using System.Diagnostics;
using System.Threading;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.Jobs.Infrastructure.Retry
{
    public class CancellableConditionalRetry<T> : IRetry<T>
    {
        private RetryPolicy<T> PollyPolicy { get; set; }
        private Logger Logger { get; set; }

        /// <summary>
        /// Retry Polly that can be cancelled if reaching maxRunTime
        /// </summary>
        /// <param name="backOff"></param>
        /// <param name="runtime"></param>
        /// <param name="maxRunTime"></param>
        public CancellableConditionalRetry(string jobGUID, IBackOffStrategy backOff, Stopwatch runtime, TimeSpan maxRunTime, Func<T, bool> condition)
        {
            Logger = NLog.LogManager.GetCurrentClassLogger();

            PollyPolicy = Policy.Handle<Exception>().OrResult<T>(condition)
                .WaitAndRetry(backOff.MaxRetry, retryAttemp => { return backOff.GetNextTime(); },
                    (state, currentSleepDuration, retryCount, context) =>
                    {
                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (TimeSpan.Compare(runtime.Elapsed, maxRunTime) == 1)
                        {
                            //the runtime is greater than the max RunTime
                            var runTimeExceedMessage = $"GetPollyPolicyWithRuntime: The runtime ({runtime}) exceeded the allotted time {maxRunTime.ToString()}";

                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;

                            Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                $"{jobGUID} STOP POLLY RETRY - {runTimeExceedMessage} - Job warning from method: {methodName} with Exception: {state.Exception}. Backoff Policy retry attempt: {retryCount}", state.Exception));
                            cts.Cancel();
                        }
                        else
                        {
                            if (state.Exception != null)
                            {
                                // An exception was thrown, retrying
                                Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                    $"{jobGUID} - Job error from method: {methodName} with Exception: {state.Exception.Message}. Backoff Policy retry attempt: {retryCount}|currentSleepDuration:{currentSleepDuration}",
                                    state.Exception));
                            }
                            else
                            {
                                // the condition wasnt met
                                Logger.Log(Msg.Create(LogLevel.Warn, Logger.Name,
                                    $"{jobGUID} - The condition wasn't met from method: {methodName}. Backoff Policy retry attempt: {retryCount}|currentSleepDuration:{currentSleepDuration}"));
                            }
                        }
                    }
                );
        }

        public T Execute(Func<T> func)
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
