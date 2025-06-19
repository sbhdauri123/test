using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using Polly;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Core
{
    public class BaseApiProvider
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private readonly Action<LogLevel, string> _log = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, msg));
        private readonly ResiliencePipeline _polly;
        public BaseApiProvider(ResiliencePipeline polly)
        {
            _polly = polly;
        }

        /// <summary>
        /// Make concurrent requests with ability to track progress via custom job report item by passing value tuple of request and job-report-item
        /// </summary>
        /// <exception cref="AggregateException"></exception>
        public List<U> MakeParallelCalls<T, U>(ParallelApiCallerArguments<T, U> args, out List<T> nextPageRequestItems)
            where T : class, IApiRequest
            where U : class
        {
            List<U> output = new();
            nextPageRequestItems = new();

            if (args == null)
            {
                return output;
            }

            ConcurrentBag<U> results = new();
            ConcurrentBag<T> nextPageRequests = new();
            ConcurrentBag<Exception> exceptions = new();

            RateLimiter.ThrottleCalls(args.RequestItems, args.PermittedItems, args.WindowInMilliseconds, _log, (requests) =>
            {
                ConcurrentQueue<DateTime> successfulRequestTimestamps = new();

                if (args.MaxRunTimeReachedFunction?.Invoke() == true)
                {
                    return (false, successfulRequestTimestamps);
                }

                Parallel.ForEach(requests, args.ParallelOptions, (request, state) =>
                {
                    try
                    {
                        U response = args.StreamResultAction != null
                            ? _polly.Execute(() => request.FetchDataAsync<U>((stream) => args.StreamResultAction?.Invoke(stream, request)).GetAwaiter().GetResult())
                            : _polly.Execute(() => request.FetchDataAsync<U>(args.DeserializeHeaders).GetAwaiter().GetResult());

                        successfulRequestTimestamps.Enqueue(DateTime.UtcNow);

                        T nextPageRequest = args.PagingFunction?.Invoke(request, response);
                        if (nextPageRequest != null)
                        {
                            nextPageRequests.Add(nextPageRequest);
                        }
                        results.Add(response);

                        args.CurrentItemAction?.Invoke(request);
                    }
                    catch (Exception exc)
                    {
                        args.ExceptionsAction?.Invoke(request, exc);
                        exceptions.Add(exc);
                        state.Stop();
                    }
                });

                args.ResultsAction?.Invoke(results);

                if (!exceptions.IsEmpty)
                {
                    throw new AggregateException(exceptions);
                }

                output.AddRange(results);

                return (true, successfulRequestTimestamps);
            });

            nextPageRequestItems.AddRange(nextPageRequests);

            return output;
        }

        public List<U> MakeParallelCallsWithPaging<T, U>(ParallelApiCallerArguments<T, U> args)
            where T : class, IApiRequest
            where U : class
        {
            List<U> output = new();

            if (args == null)
            {
                return output;
            }

            List<T> nextPageRequests;

            do
            {
                MakeParallelCalls(args, out nextPageRequests);
                args.RequestItems = nextPageRequests;
            } while (nextPageRequests.Count > 0);

            return output;
        }
    }
}
