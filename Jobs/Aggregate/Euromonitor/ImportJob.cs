using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.Euromonitor;
using Greenhouse.Data.DataSource.Euromonitor;
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

namespace Greenhouse.Jobs.Aggregate.Euromonitor
{
    [Export("Euromonitor-AggregateImportJob", typeof(IDragoJob))]
    public class ImportJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private readonly CancellationTokenSource _cts = new();
        private ITokenApiClient _tokenApiClient;
        private IEnumerable<APIReport<ReportSettings>> _apiReports;
        private EuromonitorService _euromonitorService;
        private List<OrderedQueue> _queueItems;
        private TimeSpan _maxRuntime;
        private DateTime _latestReportDate;
        private int _maxRetry;
        private int _counter;
        private int _warningCount;
        private int _exceptionCount;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();

            LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

            _tokenApiClient = new TokenApiClient(HttpClientProvider, TokenCache, CurrentCredential);
            _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
            _latestReportDate = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.EUROMONITOR_LATEST_REPORT_DATE, DateTime.MinValue);
            _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_MAX_RUNTIME, new TimeSpan(48, 0, 0));
            _maxRetry = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_POLLY_MAX_RETRY, 10);
            _counter = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_POLLY_COUNTER, 3);
            _cts.CancelAfter(_maxRuntime);

            int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
            _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();

            var euromonitorServiceArguments = new EuromonitorServiceArguments(HttpClientProvider, GreenhouseS3Creds, CurrentCredential, _tokenApiClient, CurrentIntegration.EndpointURI, GetS3PathHelper, GetDatedPartition, UploadToS3, DeleteRawFiles, LogMessage, LogException, _cts.Token);
            _euromonitorService = new EuromonitorService(euromonitorServiceArguments);
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

            foreach (var queueItem in _queueItems)
            {
                try
                {
                    var industryCodes = Enumerable.Empty<string>();
                    var geographyIds = Enumerable.Empty<int>();
                    var shareTypes = Enumerable.Empty<ShareType>();

                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                    LogMessage(LogLevel.Info, $"{nameof(EuromonitorService)} initialized.");

                    foreach (var apiReport in _apiReports.OrderBy(r => r.ReportSettings.Order))
                    {
                        if (_cts.IsCancellationRequested || TimeSpan.Compare(runTime.Elapsed, _maxRuntime) == 1)
                        {
                            LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {runTime.ElapsedMilliseconds}ms");
                            _warningCount++;
                            break;
                        }

                        var backOffStrategy = new BackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry };
                        var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, runTime, _maxRuntime);

                        if (apiReport.APIReportName.Equals(nameof(ReportName.Category)))
                        {
                            industryCodes = _euromonitorService.GenerateCategoryReport(queueItem, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.ShareType)))
                        {
                            shareTypes = _euromonitorService.GenerateShareTypeReport(queueItem, industryCodes, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.Geography)))
                        {
                            geographyIds = _euromonitorService.GenerateGeographyReport(queueItem, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.DataType)))
                        {
                            _euromonitorService.GenerateDataTypeReport(queueItem, industryCodes, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.MarketSize)))
                        {
                            _euromonitorService.GenerateMarketSizeReport(queueItem, industryCodes, geographyIds, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.CompanyShare)))
                        {
                            var companyShareTypes = shareTypes.Where(s => s.Type == nameof(ReportName.CompanyShare));
                            _euromonitorService.GenerateCompanyShareReport(queueItem, industryCodes, geographyIds, companyShareTypes, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.BrandShare)))
                        {
                            var brandShareTypes = shareTypes.Where(s => s.Type == nameof(ReportName.BrandShare));
                            _euromonitorService.GenerateBrandShareReport(queueItem, industryCodes, geographyIds, brandShareTypes, apiReport, cancellableRetry.Execute);
                        }
                    }

                    LookupService.SaveJsonObject(Constants.EUROMONITOR_LATEST_REPORT_DATE, DateTime.Today.ToString("yyyy-MM-dd"));
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Complete);
                    LogMessage(LogLevel.Info, $"{nameof(EuromonitorService)} finalized.");
                }
                catch (Exception ex)
                {
                    _exceptionCount++;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    LogException(LogLevel.Error, $"Report processing failed: GUID: {queueItem.FileGUID}. Exception: {ex.Message}", ex);
                }
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
