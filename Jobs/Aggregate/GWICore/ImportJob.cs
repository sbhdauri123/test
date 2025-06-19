using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.GWICore;
using Greenhouse.Data.DataSource.GWICore;
using Greenhouse.Data.DataSource.GWICore.Requests;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Framework;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;

namespace Greenhouse.Jobs.Aggregate.GWICore;

[Export("GWICore-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private GWICoreService _gwiCoreService;
    private List<OrderedQueue> _queueItems;
    private DateTime _latestReportDate;
    private TimeSpan _maxRuntime;
    private int _maxDegreeOfParallelism;
    private int _maxRetry;
    private int _counter;
    private int _exceptionCount;

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();

        LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        _latestReportDate = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.GWICORE_LATEST_REPORT_DATE, DateTime.MinValue);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.GWICORE_MAX_RUNTIME, new TimeSpan(0, 1, 0, 0));
        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.GWICORE_MAX_DEGREE_PARALLELISM, 4);
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.GWICORE_POLLY_MAX_RETRY, 10);
        _counter = LookupService.GetLookupValueWithDefault(Constants.GWICORE_POLLY_COUNTER, 3);

        var nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();

        var gWICoreServiceArguments = new GWICoreServiceArguments(HttpClientProvider, CurrentCredential, GreenhouseS3Creds, CurrentIntegration, GetS3PathHelper, _apiReports, _maxDegreeOfParallelism, UploadToS3, LogMessage, LogException);
        _gwiCoreService = new GWICoreService(gWICoreServiceArguments);
    }

    public void Execute()
    {
        var runTime = new Stopwatch();
        runTime.Start();

        if (_queueItems.Count == 0)
        {
            LogMessage(LogLevel.Info, "There are no items in the Queue.");
            runTime.Stop();
            return;
        }

        if (_latestReportDate.Equals(DateTime.Today))
        {
            LogMessage(LogLevel.Info, "Today's reports have already been generated.");
            UpdateQueueWithDelete(_queueItems, Constants.JobStatus.Complete, true);
            return;
        }

        var exponentialBackOffStrategy = new ExponentialBackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry };
        var queueItem = _queueItems.FirstOrDefault();

        try
        {
            var categories = new List<CategoryFilter>();
            var namespaces = new List<Namespace>();

            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
            LogMessage(LogLevel.Info, $"{nameof(GWICoreService)} initialized.");

            foreach (var apiReport in _apiReports.OrderBy(r => r.ReportSettings.Order))
            {
                if (TimeSpan.Compare(runTime.Elapsed, _maxRuntime) == 1)
                {
                    LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {runTime.ElapsedMilliseconds}ms");
                    JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                    break;
                }

                var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), exponentialBackOffStrategy, runTime, _maxRuntime);

                if (apiReport.APIReportName.Equals(nameof(ReportName.CategoriesFilter)))
                {
                    categories = _gwiCoreService.GenerateCategoriesFilterReport(queueItem, cancellableRetry.Execute);
                }
                else if (apiReport.APIReportName.Equals(nameof(ReportName.CategoryDetail)))
                {
                    _gwiCoreService.GenerateCategoryDetailReport(queueItem, categories, cancellableRetry.Execute);
                }
                else if (apiReport.APIReportName.Equals(nameof(ReportName.QuestionFilter)))
                {
                    _gwiCoreService.GenerateQuestionFilterReport(queueItem, cancellableRetry.Execute);
                }
                else if (apiReport.APIReportName.Equals(nameof(ReportName.NamespacesFilter)))
                {
                    namespaces = _gwiCoreService.GenerateNamespacesFilterReport(queueItem, cancellableRetry.Execute);
                }
                else if (apiReport.APIReportName.Equals(nameof(ReportName.LocationsFilter)))
                {
                    _gwiCoreService.GenerateLocationsFilterReport(queueItem, namespaces, cancellableRetry.Execute);
                }
                else if (apiReport.APIReportName.Equals(nameof(ReportName.SplittersFilters)))
                {
                    _gwiCoreService.GenerateSplittersFiltersReport(queueItem, namespaces, cancellableRetry.Execute);
                }
                else if (apiReport.APIReportName.Equals(nameof(ReportName.WavesFilter)))
                {
                    _gwiCoreService.GenerateWavesFilterReport(queueItem, namespaces, cancellableRetry.Execute);
                }
            }

            LookupService.SaveJsonObject(Constants.GWICORE_LATEST_REPORT_DATE, DateTime.Today.ToString("yyyy-MM-dd"));

            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Complete);
            LogMessage(LogLevel.Info, $"{nameof(GWICoreService)} finalized.");
        }
        catch (Exception ex)
        {
            _exceptionCount++;
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
            LogException(LogLevel.Error, $"Report processing failed: GUID: {queueItem.FileGUID}. Exception: {ex.Message}", ex);
        }

        runTime.Stop();
        LogMessage(LogLevel.Info, $"Import job completed. Took {runTime.Elapsed}ms to finish.");

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
    }

    public void PostExecute()
    {
    }

    private void LogMessage(LogLevel logLevel, string message)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
    }

    private void LogException(LogLevel logLevel, string message, Exception exc = null)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message), exc));
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}
