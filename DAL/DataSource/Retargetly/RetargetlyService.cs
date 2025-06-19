using Greenhouse.Common;
using Greenhouse.Data.DataSource.Retargetly;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Retargetly;

public class RetargetlyService
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _greenhouseS3Credential;
    private readonly Credential _credential;
    private readonly RetargetlyOAuth _oAuth;
    private readonly Integration _integration;
    private readonly Func<string, DateTime, string, string> _getS3PathHelper;
    private readonly string _reportExtension = ".json";
    private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;
    private readonly CancellationToken _cts;
    private readonly TimeSpan _maxRuntime;

    public RetargetlyService(RetargetlyServiceArguments serviceArguments)
    {
        _httpClientProvider = serviceArguments.HttpClientProvider;
        _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
        _credential = serviceArguments.Credential;
        _oAuth = serviceArguments.OAuth;
        _integration = serviceArguments.Integration;
        _getS3PathHelper = serviceArguments.GetS3PathHelper;
        _uploadToS3 = serviceArguments.UploadToS3;
        _logMessage = serviceArguments.LogMessage;
        _logException = serviceArguments.LogException;
        _cts = serviceArguments.cts;

        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.RETARGETLY_MAX_RUNTIME, new TimeSpan(0, 10, 0));
    }

    public void GenerateReports(IEnumerable<APIReport<ReportSettings>> apiReports, Stopwatch runTime, OrderedQueue queueItem, Action<Action> cancellableRetry, ref int warningCount)
    {
        var fileCollection = new List<FileCollectionItem>();

        foreach (var apiReport in apiReports.OrderBy(r => r.ReportSettings.Order))
        {
            if (_cts.IsCancellationRequested || TimeSpan.Compare(runTime.Elapsed, _maxRuntime) == 1)
            {
                _logMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {runTime.ElapsedMilliseconds}ms");
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
                warningCount++;
                return;
            }

            var fileCollectionItem = DownloadReport(queueItem, apiReport, cancellableRetry);
            fileCollection.Add(fileCollectionItem);
        }

        queueItem.FileCollectionJSON = JsonConvert.SerializeObject(fileCollection);
        queueItem.DeliveryFileDate = DateTime.Now;
        queueItem.FileSize = fileCollection.Sum(f => f.FileSize);
        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update((Queue)queueItem);

        LookupService.SaveJsonObject(Constants.RETARGETLY_LATEST_REPORT_DATE, DateTime.Today.ToString("yyyy-MM-dd"));
    }

    private FileCollectionItem DownloadReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{apiReport.APIReportName} initialized.");

        var fileCollectionItem = new FileCollectionItem();
        var reportName = string.Concat($"{queueItem.FileGUID}-{apiReport.APIReportName}-0".ToLower(), _reportExtension);
        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
        var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

        try
        {
            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    Headers = new Dictionary<string, string>
                    {
                        { "x-api-key", _credential.CredentialSet.XApiKey },
                        { "Authorization", $"Bearer {_oAuth.AccessToken}" }
                    }
                }, _cts).GetAwaiter().GetResult();

                if (responseStream.Length <= 0)
                {
                    return;
                }

                StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });

            fileCollectionItem.SourceFileName = apiReport.APIReportName;
            fileCollectionItem.FilePath = reportFile.FullName;
            fileCollectionItem.FileSize = reportFile.Length;
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{apiReport.APIReportName} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{apiReport.APIReportName} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{apiReport.APIReportName} finalized.");
        return fileCollectionItem;
    }
}
