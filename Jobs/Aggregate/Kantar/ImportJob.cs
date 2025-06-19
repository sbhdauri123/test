using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.Kantar;
using Greenhouse.Data.DataSource.Kantar;
using Greenhouse.Data.DataSource.Kantar.Responses;
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
using System.Threading;

namespace Greenhouse.Jobs.Aggregate.Kantar
{
    [Export("Kantar-TGI-AggregateImportJob", typeof(IDragoJob))]
    public class ImportJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private readonly CancellationTokenSource _cts = new();
        private IEnumerable<APIReport<ReportSettings>> _apiReports;
        private KantarService _kantarService;
        private List<OrderedQueue> _queueItems;
        private DateTime _latestReportDate;
        private TimeSpan _maxRuntime;
        private string _taxonomyRootLevel;
        private int _maxDegreeOfParallelism;
        private int _maxRetry;
        private int _counter;
        private int _warningCount;
        private int _exceptionCount;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();

            LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

            _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
            _latestReportDate = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.KANTAR_TGI_LATEST_REPORT_DATE, DateTime.MinValue);
            _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.KANTAR_TGI_MAX_RUNTIME, new TimeSpan(23, 0, 0));
            _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.KANTAR_TGI_MAX_DEGREE_PARALLELISM, 8);
            _maxRetry = LookupService.GetLookupValueWithDefault(Constants.KANTAR_TGI_POLLY_MAX_RETRY, 10);
            _counter = LookupService.GetLookupValueWithDefault(Constants.KANTAR_TGI_POLLY_COUNTER, 3);
            _taxonomyRootLevel = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.KANTAR_TGI_TAXONOMY_ROOT_LEVEL, "root");
            _cts.CancelAfter(_maxRuntime);

            var nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
            _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();

            var kantarServiceArguments = new KantarServiceArguments(HttpClientProvider, CurrentCredential, GreenhouseS3Creds, CurrentIntegration, GetS3PathHelper, _apiReports, _taxonomyRootLevel, _maxDegreeOfParallelism, UploadToS3, LogMessage, LogException, _cts.Token);
            _kantarService = new KantarService(kantarServiceArguments);
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

            var exponentialBackOffStrategy = new BackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry };
            var queueItem = _queueItems.FirstOrDefault();

            try
            {
                var surveys = new List<Survey>();

                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                LogMessage(LogLevel.Info, $"{nameof(KantarService)} initialized.");

                foreach (var apiReport in _apiReports.OrderBy(r => r.ReportSettings.Order))
                {
                    if (_cts.IsCancellationRequested || TimeSpan.Compare(runTime.Elapsed, _maxRuntime) == 1)
                    {
                        LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {runTime.ElapsedMilliseconds}ms");
                        JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                        _warningCount++;
                        break;
                    }

                    var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), exponentialBackOffStrategy, runTime, _maxRuntime);

                    if (apiReport.APIReportName.Equals(nameof(ReportName.Surveys)))
                    {
                        surveys = _kantarService.GenerateSurveysReport(queueItem, cancellableRetry.Execute);
                    }
                    else if (apiReport.APIReportName.Equals(nameof(ReportName.Survey)))
                    {
                        _kantarService.GenerateSurveyInfoReport(queueItem, surveys, cancellableRetry.Execute);
                    }
                    else if (apiReport.APIReportName.Equals(nameof(ReportName.Category)))
                    {
                        _kantarService.GenerateCategoriesReport(queueItem, surveys, cancellableRetry.Execute);
                    }
                    else if (apiReport.APIReportName.Equals(nameof(ReportName.Question)))
                    {
                        _kantarService.GenerateChildCategoriesReport(queueItem, surveys, cancellableRetry.Execute);
                    }
                }

                LookupService.SaveJsonObject(Constants.KANTAR_TGI_LATEST_REPORT_DATE, DateTime.Today.ToString("yyyy-MM-dd"));

                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Complete);
                LogMessage(LogLevel.Info, $"{nameof(KantarService)} finalized.");
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
            else if (_warningCount > 0)
            {
                JobLogger.JobLog.Status = nameof(Constants.JobLogStatus.Warning);
                JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
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
                _cts.Dispose();
            }
        }

        ~ImportJob()
        {
            Dispose(false);
        }
    }
}
