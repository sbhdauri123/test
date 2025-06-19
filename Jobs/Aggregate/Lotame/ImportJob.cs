using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.Lotame;
using Greenhouse.Data.DataSource.Lotame;
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

namespace Greenhouse.Jobs.Aggregate.Lotame
{
    [Export("Lotame-AggregateImportJob", typeof(IDragoJob))]
    public class ImportJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private readonly CancellationTokenSource _cts = new();
        private Stopwatch _runTime = new();
        private IEnumerable<APIReport<ReportSettings>> _apiReports;
        private LotameService _lotameService;
        private List<OrderedQueue> _queueItems;
        private List<DimensionState> _dimensionStates;
        private List<string> _invalidEntityIds = new();
        private TimeSpan _maxRuntime;
        private int _maxDegreeOfParallelism;
        private int _maxRetry;
        private int _counter;
        private int _pageSize;
        private int _warningCount;
        private int _exceptionCount;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();

            LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

            _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
            _dimensionStates = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.LOTAME_DIMENSION_STATE, new List<DimensionState>());
            _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.LOTAME_MAX_DEGREE_PARALLELISM, 1);
            _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.LOTAME_MAX_RUNTIME, new TimeSpan(23, 0, 0));
            _maxRetry = LookupService.GetLookupValueWithDefault(Constants.LOTAME_POLLY_MAX_RETRY, 10);
            _counter = LookupService.GetLookupValueWithDefault(Constants.LOTAME_POLLY_COUNTER, 3);
            _pageSize = LookupService.GetLookupValueWithDefault(Constants.LOTAME_PAGE_SIZE, 2500);
            _cts.CancelAfter(_maxRuntime);

            int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
            _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();

            var lotameServiceArguments = new LotameServiceArguments(HttpClientProvider, CurrentCredential, GreenhouseS3Creds, CurrentIntegration, GetS3PathHelper, _maxDegreeOfParallelism, _pageSize, UploadToS3, LogMessage, LogException, _cts.Token);
            _lotameService = new LotameService(lotameServiceArguments);
        }

        public void Execute()
        {
            _runTime.Start();

            if (_queueItems.Count == 0)
            {
                LogMessage(LogLevel.Info, "There are no items in the Queue.");
                _runTime.Stop();
                return;
            }

            foreach (var queueItem in _queueItems)
            {
                if (IsTimeoutReached)
                {
                    LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
                    _warningCount++;
                    break;
                }

                try
                {
                    var dimensionState = _dimensionStates.FirstOrDefault(d => d.EntityId == queueItem.EntityID);
                    if (dimensionState != null && dimensionState.LatestReportDate.Equals(DateTime.Today))
                    {
                        LogMessage(LogLevel.Info, $"Today's reports have already been generated for EntityID: {queueItem.EntityID}, FileDate: {queueItem.FileDate}.");
                        _queueItems.Where(q => q.EntityID == queueItem.EntityID).ToList()
                            .ForEach(item => JobService.UpdateQueueStatus(item.ID, Constants.JobStatus.Complete));
                        continue;
                    }

                    if (_invalidEntityIds.Contains(queueItem.EntityID))
                    {
                        LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-{queueItem.FileDate}-EntityID {queueItem.EntityID} has been flagged as error earlier in this Import job and will be skipped.");
                        continue;
                    }

                    var hierarchyIds = new List<Hierarchy>();
                    var nodes = new List<Node>();
                    var behaviorTypes = new List<BehaviorType>();

                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                    LogMessage(LogLevel.Info, $"{nameof(LotameService)} initialized.");

                    foreach (var apiReport in _apiReports.OrderBy(r => r.ReportSettings.Order))
                    {
                        if (IsTimeoutReached)
                        {
                            LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
                            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
                            _warningCount++;
                            break;
                        }

                        var backOffStrategy = new MultiplicativeBackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry, Seed = 60 };
                        var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);

                        if (apiReport.APIReportName.Equals(nameof(ReportName.HierarchyList)))
                        {
                            hierarchyIds = _lotameService.GenerateHierarchyListReport(queueItem, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.HierarchyIdDetail)))
                        {
                            nodes = _lotameService.GenerateHierarchyIdDetailReport(queueItem, apiReport, hierarchyIds, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.HierarchyGrouping)))
                        {
                            _lotameService.GenerateHierarchyGroupingReport(queueItem, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.HierarchyNodeDetail)))
                        {
                            _lotameService.GenerateHierarchyNodeDetailReport(queueItem, apiReport, nodes, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.BehaviorTypeList)))
                        {
                            behaviorTypes = _lotameService.GenerateBehaviorTypeListReport(queueItem, apiReport, cancellableRetry.Execute);
                        }
                        else if (apiReport.APIReportName.Equals(nameof(ReportName.BehaviorSearchList)))
                        {
                            _lotameService.GenerateBehaviorSearchListReport(queueItem, apiReport, behaviorTypes, cancellableRetry.Execute);
                        }
                    }

                    if (!IsTimeoutReached)
                    {
                        if (dimensionState != null)
                        {
                            dimensionState.LatestReportDate = DateTime.Today;
                        }
                        else
                        {
                            _dimensionStates.Add(new DimensionState { EntityId = queueItem.EntityID.ToLower(), LatestReportDate = DateTime.Today });
                        }

                        LookupService.SaveJsonObject(Constants.LOTAME_DIMENSION_STATE, _dimensionStates);
                        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Complete);
                    }

                    LogMessage(LogLevel.Info, $"{nameof(LotameService)} finalized.");
                }
                catch (Exception ex)
                {
                    _exceptionCount++;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    LogException(LogLevel.Error, $"Report processing failed: GUID: {queueItem.FileGUID}, EntityID: {queueItem.EntityID}. Exception: {ex.Message}", ex);
                    _invalidEntityIds.Add(queueItem.EntityID);
                }
            }

            _runTime.Stop();
            LogMessage(LogLevel.Info, $"Import job completed. Took {_runTime.Elapsed}ms to finish.");

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

        private bool IsTimeoutReached => _cts.IsCancellationRequested || TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1;

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