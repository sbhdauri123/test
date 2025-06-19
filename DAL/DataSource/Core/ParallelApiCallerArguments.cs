using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Core
{
    public class ParallelApiCallerArguments<T, U>
    {
        public ParallelOptions ParallelOptions { get; init; }
        public List<T> RequestItems { get; set; }
        public Action<ConcurrentBag<U>> ResultsAction { get; init; }
        public Action<T, Exception> ExceptionsAction { get; init; }
        public Func<T, U, ValueTask> ProcessFunction { get; init; }
        public Action<Stream, T> StreamResultAction { get; init; }
        public Action<T> CurrentItemAction { get; set; }
        public bool DeserializeHeaders { get; set; }
        public Func<T, U, T> PagingFunction { get; set; }
        public int PermittedItems { get; set; }
        public long WindowInMilliseconds { get; set; }
        public Func<bool> MaxRunTimeReachedFunction { get; set; }
    }
}
