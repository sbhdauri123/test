using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks.RunListResponse;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Greenhouse.DAL.Databricks;

public class DatabricksCalls : IDatabricksCalls
{
    private const string NEW_JOB_RUN_PATH = "jobs/run-now";
    private const string CHECK_JOB_STATUS_PATH = "jobs/runs/get";
    private const string CHECK_JOB_OUTPUT_PATH = "jobs/runs/get-output";
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly int _pageSize;

    private readonly Credential _credential;

    public DatabricksCalls(Credential creds, int pageSize, IHttpClientProvider httpClientProvider)
    {
        _credential = creds;
        _pageSize = pageSize;
        _httpClientProvider = httpClientProvider;
    }

    public List<Databricks.RunListResponse.Run> GetJobRunList(Func<string, string> PrefixJobGuid, string jobID)
    {
        var runs = new List<Databricks.RunListResponse.Run>();

        var databricksRequest = new DataBricksRequest(_httpClientProvider, _credential, HttpMethod.Get);

        int page = 0;
        HttpWebResponse response = null;
        bool hasMore = false;
        do
        {
            HttpWebRequest req = databricksRequest.GetRequest($"/jobs/runs/list?job_id={jobID}&{AddPagingQueryString(page++)}");
            PrefixJobGuid($"Requesting Run List at: {req.RequestUri.AbsoluteUri}");

            try
            {
                response = (HttpWebResponse)req.GetResponse();
                string data = string.Empty;
                using (var strm = new System.IO.StreamReader(response.GetResponseStream()))
                {
                    data = strm.ReadToEnd();
                }

                var result =
                    Newtonsoft.Json.JsonConvert.DeserializeObject<Databricks.RunListResponse.JobListResponse>(data);

                hasMore = result.HasMore;
                runs.AddRange(result.Runs);

                logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Info, logger.Name,
                    PrefixJobGuid(
                        $"Returning Runs count: {Newtonsoft.Json.JsonConvert.SerializeObject(result.Runs.Count)}")));
            }
            catch (Exception exc)
            {
                logger.Log(Logging.Msg.Create(NLog.LogLevel.Error, logger.Name,
                    PrefixJobGuid(
                        $"Databricks Command history exception: {exc.Message}; Inner Exception: {exc?.InnerException?.Message}"), exc));
                throw;
            }
            finally
            {
                response.Close();
                response.Dispose();
            }
        } while (hasMore);

        return runs;
    }

    /// <summary>
    /// Get the last Job run and return it if it matches the fileGUID, return NULL otherwise.
    /// IMPORTANT: Will not return the last job run with the fileGUID provided if other job run for the jobID provided have run since
    /// </summary>
    /// <returns></returns>
    public Run GetLatestJobRun(Func<string, string> PrefixJobGuid, string jobID, string fileGUID)
    {
        var databricksRequest = new DataBricksRequest(_httpClientProvider, _credential, HttpMethod.Get);

        HttpWebResponse response = null;

        HttpWebRequest req = databricksRequest.GetRequest($"/jobs/runs/list?job_id={jobID}&offset=0&limit=1&expand_tasks=true");
        PrefixJobGuid($"Requesting Run List at: {req.RequestUri.AbsoluteUri}");

        JobListResponse result;

        try
        {
            response = (HttpWebResponse)req.GetResponse();
            string data = string.Empty;
            using (var strm = new System.IO.StreamReader(response.GetResponseStream()))
            {
                data = strm.ReadToEnd();
            }

            result = Newtonsoft.Json.JsonConvert.DeserializeObject<Databricks.RunListResponse.JobListResponse>(data);

            logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Returning Runs count: {Newtonsoft.Json.JsonConvert.SerializeObject(result.Runs?.Count)}")));
        }
        catch (Exception exc)
        {
            logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Databricks Command history exception: {exc.Message}; Inner Exception: {exc?.InnerException?.Message}")));
            throw;
        }
        finally
        {
            response.Close();
            response.Dispose();
        }

        var run = result.Runs?.FirstOrDefault();
        if (run == null) return null;

        if (run.FileGUID == null)
        {
            throw new DataBricksCallsException("Databricks Parameter FileGUID Missing - Invalid format");
        }

        if (string.Equals(run.FileGUID, fileGUID, StringComparison.InvariantCultureIgnoreCase))
        {
            return run;
        }

        return null;
    }

    private string AddPagingQueryString(int page)
    {
        return $"offset={page * _pageSize}&limit={_pageSize}";
    }

    public JobRunResponse RunJob(JobRunRequest cmd)
    {
        JobRunResponse data = null;
        var databricksRequest = new DataBricksRequest(_httpClientProvider, _credential, HttpMethod.Post);

        var path = $"/{NEW_JOB_RUN_PATH}";
        var response = databricksRequest.PostRequest(path, JsonConvert.SerializeObject(cmd));

        using (var strm = new System.IO.StreamReader(response.GetResponseStream()))
        {
            var resp = strm.ReadToEnd();
            data = Newtonsoft.Json.JsonConvert.DeserializeObject<JobRunResponse>(resp);

            response.Close();
        }

        return data;
    }

    public bool DeleteJobRun(string jobRunId, Func<string, string> PrefixJobGuid)
    {
        var databricksRequest = new DataBricksRequest(_httpClientProvider, _credential, HttpMethod.Post);

        var response = databricksRequest.PostRequest("/jobs/runs/delete", "{\"run_id\": " + jobRunId + "}");

        try
        {
            using (var strm = new System.IO.StreamReader(response.GetResponseStream()))
            {
                var resp = strm.ReadToEnd();
                var data = Newtonsoft.Json.JsonConvert.DeserializeObject<DeleteJobRunResponse>(resp);
                response.Close();

                if (!string.IsNullOrEmpty(data.ErrorCode))
                {
                    throw new DataBricksCallsException($"Error Code={data.ErrorCode}, Message={data.Message}");
                }

                return true;
            }
        }
        catch (Exception exc)
        {
            logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Error, logger.Name,
                PrefixJobGuid(
                    $"Databricks Delete Job Run Command exception: {exc.Message}; Inner Exception: {exc?.InnerException?.Message}"), exc));
            throw;
        }
        finally
        {
            response.Close();
            response.Dispose();
        }
    }

    public bool RepairJobRun(string jobRunId, string[] jarParams, Func<string, string> PrefixJobGuid)
    {
        var databricksRequest = new DataBricksRequest(_httpClientProvider, _credential, HttpMethod.Post);

        var body = new RepairJobRunRequest
        {
            JobRunID = jobRunId,
            JarParams = jarParams
        };

        var response = databricksRequest.PostRequest("/jobs/runs/repair", JsonConvert.SerializeObject(body));

        try
        {
            using (var strm = new System.IO.StreamReader(response.GetResponseStream()))
            {
                var resp = strm.ReadToEnd();
                var data = Newtonsoft.Json.JsonConvert.DeserializeObject<DeleteJobRunResponse>(resp);
                response.Close();

                if (!string.IsNullOrEmpty(data.ErrorCode))
                {
                    throw new DataBricksCallsException($"Error Code={data.ErrorCode}, Message={data.Message}");
                }

                return true;
            }
        }
        catch (Exception exc)
        {
            logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Error, logger.Name,
                PrefixJobGuid(
                    $"Databricks Delete Job Run Command exception: {exc.Message}; Inner Exception: {exc?.InnerException?.Message}")));
            throw;
        }
        finally
        {
            response.Close();
            response.Dispose();
        }
    }

    public Run GetJobStatus(string jobRunId, Func<string, string> PrefixJobGuid)
    {
        var databricksRequest = new DataBricksRequest(_httpClientProvider, _credential, HttpMethod.Get);

        HttpWebResponse response = null;
        HttpWebRequest req = databricksRequest.GetRequest($"/jobs/runs/get?run_id={jobRunId}");
        Run run;

        try
        {
            response = (HttpWebResponse)req.GetResponse();
            string data = string.Empty;
            using (var strm = new System.IO.StreamReader(response.GetResponseStream()))
            {
                data = strm.ReadToEnd();
            }

            run = Newtonsoft.Json.JsonConvert.DeserializeObject<Run>(data);

            logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Returned result:{data}")));
        }
        catch (Exception exc)
        {
            logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Databricks Command history exception: {exc.Message}; Inner Exception: {exc?.InnerException?.Message}")));
            throw;
        }
        finally
        {
            if (response != null)
            {
                response.Close();
                response.Dispose();
            }
        }

        return run;
    }

    /// <summary>
    /// Submit a Databricks job request asyncronously
    /// </summary>
    public async Task<JobRunResponse> RunJobAsync(JobRunRequest jobParams)
    {
        DataBricksRequest databricksRequest = new(_httpClientProvider, _credential, HttpMethod.Post);
        string bodyRequest = JsonConvert.SerializeObject(jobParams, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        string response = await databricksRequest.SendRequestAsync(NEW_JOB_RUN_PATH, bodyRequest);
        return JsonConvert.DeserializeObject<JobRunResponse>(response);
    }

    /// <summary>
    /// Check the status of a Databricks job run asyncronously
    /// </summary>
    public async Task<ResultState> CheckJobStatusAsync(long jobRunID)
    {
        DataBricksRequest databricksRequest = new(_httpClientProvider, _credential, HttpMethod.Get);
        string endpoint = $"{CHECK_JOB_STATUS_PATH}?run_id={jobRunID}";
        string response = await databricksRequest.SendRequestAsync(endpoint, null);
        var result = JsonConvert.DeserializeObject<Run>(response);
        // Queued/Skipped depends on whether or not the Databricks has the "Queue" option enabled
        // QUEUED is not considered a "done" status, so status will keep checking until job runs
        // SKIPPED is considered a "done" status and will mark queue as error
        switch (result.State.LifeCycleState)
        {
            case "QUEUED":
                return ResultState.QUEUED;
            case "SKIPPED":
                return ResultState.SKIPPED;
            case "INTERNAL_ERROR":
                var failedTasks = result.Tasks.Where(t => t.State.ResultState == "FAILED") ?? [];
                var errorMessages = await GetErrorMessagesForFailedTasksAsync(failedTasks, databricksRequest, result.JobId);
                throw new DatabricksResultNotSuccessfulException(errorMessages);
            default:
                break;
        }
        return result.State.LifeCycleState == "RUNNING" ? ResultState.WAITING : result.State.ResultStateEnum;
    }


    public static async Task<string> GetErrorMessagesForFailedTasksAsync(IEnumerable<RunListResponse.JobTask> tasks, DataBricksRequest databricksRequest, string jobId)
    {
        var errorMessages = await Task.WhenAll(tasks.Select(async task =>
        {
            string outputEndpoint = $"{CHECK_JOB_OUTPUT_PATH}?run_id={task.RunId}";
            string outputResponse;
            try
            {
                outputResponse = await databricksRequest.SendRequestAsync(outputEndpoint, null);
            }
            catch (Exception exc)
            {
                logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Error, logger.Name,
                    $"Databricks -> GetErrorMessagesForFailedTasks exception: {exc.Message};"));
                return $"Task {task.RunId} failed, URL: {task.RunPageUrl}";
            }
            var outputResult = JsonConvert.DeserializeObject<Response>(outputResponse);
            return $"Databricks Job Run Failed: Job ID: {jobId}, Run ID: {task.RunId}, Error Message: {outputResult.Error}, Run Page URL: {task.RunPageUrl}";
        }));
        return string.Join(System.Environment.NewLine, errorMessages);
    }
}

[Serializable]
internal sealed class DataBricksCallsException : Exception
{
public DataBricksCallsException()
{
}

public DataBricksCallsException(string message) : base(message)
{
}

public DataBricksCallsException(string message, Exception innerException) : base(message, innerException)
{
}
}
