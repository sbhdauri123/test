using Greenhouse.DAL.Databricks.RunListResponse;
using System;
using System.Threading.Tasks;

namespace Greenhouse.DAL.Databricks
{
    public interface IDatabricksCalls
    {
        public JobRunResponse RunJob(JobRunRequest cmd);
        public Run GetJobStatus(string jobRunId, Func<string, string> PrefixJobGuid);
        public Task<JobRunResponse> RunJobAsync(JobRunRequest jobParams);
        public Task<ResultState> CheckJobStatusAsync(long jobRunID);
    }
}
